package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
	"github.com/tidwall/gjson"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	// AWS Application Load Balancers
	// source:  https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
	// format:  bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
	// example: my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz
	// VPC Flow Logs
	// source: https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs-s3.html#flow-logs-s3-path
	// format: bucket-and-optional-prefix/AWSLogs/account_id/vpcflowlogs/region/year/month/day/aws_account_id_vpcflowlogs_region_flow_log_id_YYYYMMDDTHHmmZ_hash.log.gz
	// example: 123456789012_vpcflowlogs_us-east-1_fl-1234abcd_20180620T1620Z_fe123456.log.gz
	filenameRegex         = regexp.MustCompile(`AWSLogs\/(?P<account_id>\d+)\/(?P<type>\w+)\/(?P<region>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/\d+\_(?:elasticloadbalancing|vpcflowlogs)\_\w+-\w+-\d_(?:(?:app|nlb|net)\.*?)?(?P<src>[a-zA-Z0-9\-]+)`)
	filenameRegexFirewall = regexp.MustCompile(`(?P<log_type>\w+)\/AWSLogs\/(?P<account_id>\d+)\/(?P<type>[\w-]+)\/(?P<src>\w+)\/(?P<region>[\w-]+)\/(?P<firewall_name>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/(?P<hour>\d+)\/(?P<log_file>.*)`)
	filenameRegexWAF      = regexp.MustCompile(`AWSLogs\/year\=(?P<year>\d+)\/month\=(?P<month>\d+)\/day\=(?P<day>\d+)\/hour\=(?P<hour>\d+)\/(?P<src>.*)`)
	filenameRegexRDS      = regexp.MustCompile(`(?P<rds_instance>.*)\/(?P<log_type>.*)\/year\=(?P<year>\d+)\/month\=(?P<month>\d+)\/day\=(?P<day>\d+)\/hour\=(?P<hour>\d+)\/(?P<src>.*)`)

	// regex that extracts the timestamp (RFC3339) from message log
	timestampRegex = regexp.MustCompile(`\w+ (?P<timestamp>\d+-\d+-\d+T\d+:\d+:\d+\.\d+Z)`)
)

const (
	FLOW_LOG_TYPE             string = "vpcflowlogs"
	LB_LOG_TYPE               string = "elasticloadbalancing"
	NETWORK_FIREWALL_LOG_TYPE string = "network-firewall"
	WAF_LOG_TYPE              string = "waf"
	RDS_LOG_TYPE              string = "rds"
)

func getS3Client(ctx context.Context, region string) (*s3.Client, error) {
	var s3Client *s3.Client

	if c, ok := s3Clients[region]; ok {
		s3Client = c
	} else {

		if os.Getenv("MODE") == "DEV" {
			cfg, err := config.LoadDefaultConfig(context.TODO(),
				config.WithRegion(region),
				config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("ASIA4WBMAG4C6BPAXTXQ", "7aCHekUmYGLhyWEqvrGLxkX0dUE36mcyDDUEm4wK", "IQoJb3JpZ2luX2VjEOL//////////wEaCXVzLWVhc3QtMSJHMEUCIBasZUhFqSOoUF2Mk2F8/Oh7lH5GAXHHmU0Pc0rrM5xUAiEAmDSQ566obKwgMshID5DCzPx8iPTqmJIX7C+pH4NZvxYqlwQI6///////////ARABGgw4NzE5NzA2NDk4NjEiDDK0MeguKknFHOAFdCrrA/07bSg8ySi2zcAT3amNVGaCtQo5O81O2mWOcZjPgJy0Oe7WSLhX/Eio0gZkdklb+10YvFV+PKjqHDAxZTeMgIvBKcQOKVoPj50yt2ZSFrs5XgmqyLlkrj/5C5chBiQNajK8etTb67MOfrQSSZYQ0Wq/AC1QMLQE9pelXEEq0QPu7T/cbkhpPAQH/4pwOYS9sEs5JOpLDFe82vzqwOLVNzVJ3Kvz/u3uruOG0RX9PQrNH6mE7NZPI/IZHERv0h3QPFIjRT5nWCtoZybysKciao4UmWuku96ZKxQUkdso5sCM4sL/75OHAfQjWRi6u63yWpK6SWcR6lAKJnxVFWTP9ERMLOQDUOs1xOVgidCcFazbP5bSlnhyX+Pua9fyO5RdLKqAKpq3nDcaGDV+rwT6yo7WHjaGPMG9JK1s6QoJZvvDyA3I41K85iQrY1f701ISV7wQjlqOaVUzHbFJoeo2fqY3tJF5Uo2bPkfjAyAi2sZaqFysaSAoh4IyMlnuy0ZOSxcap4efaCIms2UfbQMZ99qR9AZCBBg66azYhu24ooRKe1P7AZe6gv0BKl1satsdTkmUDQIrew+3MqKRYv+1KV82zkseIg4f/seTUi8VUcInzW3ck2GLIhbTr5Tsz/6yq/fvILlaCpcg6/tqMKeBzqIGOqYBwtbk0wLfoUo8BqwJg+Y5roZtjY7uLHkeOfeyaUvf/m6aDGSCAAZ2yQ1y0lTYu+Lc/VZ4GuWWKHCpKbGfsugyl2yIMhcvWTa02GghE4Y5PDbRXrrT9cDhbb9DfQ/PfyDxoIQ2f+OG6+/FcjWXrUy6ofei3pM61OX8CwZnZuMqk459Pivxy0EVWT/cT/HNQfWa4HZpLdzpI6miGso1kvTCXak/BW607g==")),
			)
			if err != nil {
				return nil, err
			}
			s3Client = s3.NewFromConfig(cfg)
			s3Clients[region] = s3Client
		} else {
			cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
			if err != nil {
				return nil, err
			}
			s3Client = s3.NewFromConfig(cfg)
			s3Clients[region] = s3Client

		}

	}
	return s3Client, nil
}

func parseS3Log(ctx context.Context, b *batch, labels map[string]string, obj io.ReadCloser) error {
	var scanner *bufio.Scanner
	if !strings.Contains(labels["key"], "gz") {
		scanner = bufio.NewScanner(obj)
	} else {
		gzreader, errGzip := gzip.NewReader(obj)
		scanner = bufio.NewScanner(gzreader)
		if errGzip != nil {
			return errGzip
		}
	}

	skipHeader := false
	logType := labels["type"]
	if labels["type"] == FLOW_LOG_TYPE {
		skipHeader = true
		logType = "s3_vpc_flow"
	} else if labels["type"] == LB_LOG_TYPE {
		logType = "s3_lb"
	} else if labels["type"] == RDS_LOG_TYPE {
		logType = "s3_rds_" + labels["log_type"]
	} else if labels["type"] == WAF_LOG_TYPE {
		logType = "s3_waf"
	} else if labels["type"] == NETWORK_FIREWALL_LOG_TYPE {
		logType = "s3_network_firewall"
	}

	ls := model.LabelSet{
		model.LabelName("__aws_log_type"):                       model.LabelValue(logType),
		model.LabelName(fmt.Sprintf("__aws_%s", logType)):       model.LabelValue(labels["src"]),
		model.LabelName(fmt.Sprintf("__aws_%s_owner", logType)): model.LabelValue(labels["account_id"]),
	}

	ls = applyExtraLabels(ls)

	timestamp := time.Now()
	var lineCount int
	for scanner.Scan() {
		log_line := scanner.Text()
		lineCount++
		if lineCount == 1 && skipHeader {
			continue
		}
		if printLogLine {
			fmt.Println(log_line)
		}

		match := timestampRegex.FindStringSubmatch(log_line)
		if len(match) > 0 {
			timestampLog, err := time.Parse(time.RFC3339, match[1])
			timestamp = timestampLog
			if err != nil {
				return err
			}
		}
		if labels["type"] == RDS_LOG_TYPE {
			if strings.Contains(labels["log_type"], "audit") {
				match = strings.Split(log_line, ",")
				tsStr := match[0]
				tsStr = tsStr[:len(tsStr)-6]
				intTime, errInt := strconv.ParseInt(tsStr, 10, 64)
				if errInt == nil {
					timestamp = time.Unix(intTime, 0)
				}
			}
			match = strings.Split(log_line, " ")
			tsStr := match[0]
			ts, err := time.Parse("2006-01-02T15:04:05.000000Z", tsStr)
			if err == nil {
				timestamp = ts
			}
		}
		if labels["type"] == WAF_LOG_TYPE {
			tsJson := gjson.Get(log_line, "timestamp")
			tsStr := tsJson.String()
			tsStr = tsStr[:len(tsStr)-3]
			intTime, errInt := strconv.ParseInt(tsStr, 10, 64)
			if errInt == nil {
				timestamp = time.Unix(intTime, 0)
			}

		}
		if labels["type"] == NETWORK_FIREWALL_LOG_TYPE {
			tsJson := gjson.Get(log_line, "event_timestamp")
			tsStr := tsJson.String()
			// tsStr = tsStr[:len(tsStr)]
			intTime, errInt := strconv.ParseInt(tsStr, 10, 64)
			if errInt == nil {
				timestamp = time.Unix(intTime, 0)
			}

		}
		log_labels:= parser_json(log_line)
		ls=ls.Merge(log_labels)
		if err := b.add(ctx, entry{ls, logproto.Entry{
			Line:      log_line,
			Timestamp: timestamp,
		}}); err != nil {
			return err
		}
	}

	return nil
}

func getLabels(record events.S3EventRecord) (map[string]string, error) {

	labels := make(map[string]string)

	labels["key"] = record.S3.Object.Key
	labels["bucket"] = record.S3.Bucket.Name
	labels["bucket_owner"] = record.S3.Bucket.OwnerIdentity.PrincipalID
	labels["bucket_region"] = record.AWSRegion
	decodeKey, err := url.PathUnescape(labels["key"])
	if err == nil {
		labels["key"] = decodeKey
	}
	match := filenameRegex.FindStringSubmatch(labels["key"])
	if len(match) > 0 {
		for i, name := range filenameRegex.SubexpNames() {
			if i != 0 && name != "" {
				labels[name] = match[i]
			}
		}
	}
	match = filenameRegexFirewall.FindStringSubmatch(labels["key"])
	if len(match) > 0 {
		for i, name := range filenameRegexFirewall.SubexpNames() {
			if i != 0 && name != "" {
				labels[name] = match[i]
			}
		}
	}
	match = filenameRegexRDS.FindStringSubmatch(labels["key"])
	if len(match) > 0 {
		for i, name := range filenameRegexRDS.SubexpNames() {
			if i != 0 && name != "" {
				labels[name] = match[i]
			}
		}
		labels["type"] = RDS_LOG_TYPE
		labels["src"] = record.S3.Bucket.Name
		labels["account_id"] = record.S3.Bucket.OwnerIdentity.PrincipalID
	}
	match = filenameRegexWAF.FindStringSubmatch(labels["key"])
	if len(match) > 0 {
		for i, name := range filenameRegexWAF.SubexpNames() {
			if i != 0 && name != "" {
				labels[name] = match[i]
			}
		}
		labels["type"] = WAF_LOG_TYPE
		labels["src"] = record.S3.Bucket.Name
		labels["account_id"] = record.S3.Bucket.OwnerIdentity.PrincipalID
	}

	return labels, nil
}

func processS3Event(ctx context.Context, ev *events.S3Event, pc Client, log *log.Logger) error {
	batch, err := newBatch(ctx, pc)
	if err != nil {
		return err
	}
	for _, record := range ev.Records {
		labels, err := getLabels(record)
		decodeKey, err := url.PathUnescape(labels["key"])
		if err == nil {
			labels["key"] = decodeKey
		}
		if err != nil {
			return err
		}
		level.Info(*log).Log("msg", fmt.Sprintf("fetching s3 file: %s", labels["key"]))
		s3Client, err := getS3Client(ctx, labels["bucket_region"])
		if err != nil {
			return err
		}
		obj, err := s3Client.GetObject(ctx,
			&s3.GetObjectInput{
				Bucket:              aws.String(labels["bucket"]),
				Key:                 aws.String(labels["key"]),
				ExpectedBucketOwner: aws.String(labels["bucketOwner"]),
			})
		if err != nil {
			return fmt.Errorf("Failed to get object %s from bucket %s on account %s\n, %s", labels["key"], labels["bucket"], labels["bucketOwner"], err)
		}
		err = parseS3Log(ctx, batch, labels, obj.Body)
		if err != nil {
			return err
		}
	}

	err = pc.sendToPromtail(ctx, batch)
	if err != nil {
		return err
	}

	return nil
}
