package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/url"
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

	// "github.com/aws/aws-sdk-go-v2/credentials"
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
	filenameRegex    = regexp.MustCompile(`AWSLogs\/(?P<account_id>\d+)\/(?P<type>\w+)\/(?P<region>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/\d+\_(?:elasticloadbalancing|vpcflowlogs)\_\w+-\w+-\d_(?:(?:app|nlb|net)\.*?)?(?P<src>[a-zA-Z0-9\-]+)`)
	filenameRegexWAF = regexp.MustCompile(`AWSLogs\/year\=(?P<year>\d+)\/month\=(?P<month>\d+)\/day\=(?P<day>\d+)\/hour\=(?P<hour>\d+)\/(?P<src>.*)`)
	filenameRegexRDS = regexp.MustCompile(`(?P<rds_instance>.*)\/(?P<log_type>.*)\/year\=(?P<year>\d+)\/month\=(?P<month>\d+)\/day\=(?P<day>\d+)\/hour\=(?P<hour>\d+)\/(?P<src>.*)`)

	// regex that extracts the timestamp (RFC3339) from message log
	timestampRegex = regexp.MustCompile(`\w+ (?P<timestamp>\d+-\d+-\d+T\d+:\d+:\d+\.\d+Z)`)
)

const (
	FLOW_LOG_TYPE string = "vpcflowlogs"
	LB_LOG_TYPE   string = "elasticloadbalancing"
	WAF_LOG_TYPE  string = "waf"
	RDS_LOG_TYPE  string = "rds"
)

func getS3Client(ctx context.Context, region string) (*s3.Client, error) {
	var s3Client *s3.Client

	if c, ok := s3Clients[region]; ok {
		s3Client = c
	} else {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		// cfg, err := config.LoadDefaultConfig(context.TODO(),
		// config.WithRegion(region),
		// 	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("ASIAYZ3IT4J6EWAK346R", "B5sHPZG2boj2rY6wmO5q2cEaXpj5meDgWBOxcV9O", "IQoJb3JpZ2luX2VjEKv//////////wEaCXVzLWVhc3QtMSJIMEYCIQDSJsWqpnGlQ+TUmA+Y6u3SZgPSy1zEbKY6tDGdQ3+9cQIhAP06hQGudRG41kpuhLN//lKg1jxkXCF1AbFQ5VNNNv5nKpcECKT//////////wEQABoMNjA1MjcyOTI0Nzk2Igz9sgQA+5KtkdycesIq6wPOFcFiYJZK7skJt/ZHd5jMWeufCjpEXOZuk/xUGy2nUBje8L5/IVdi1Qdr/6CUFFOSGe4r0DYr+7m972RdDz2wRIrbThhnd6A0zixdBkybEh4Nww4nHztoJZlM/0eAtvR4ck2wu4x/e+1II037yQyczCqt7nTxQGltaV8GEwk1/nbxRzpph9bjXP65Ii8kCCCSJQVysPjz7NDEKIN+EX0KfUNH82LFXNtfzsD/7Ie92qJJrm1E4Ku5HSNOv+FSdI4rW1UAgngoKHP2ZkQAY5Ntgo+QfHbrlvFpMWlkknlbhuOo/1rI8UTN5DXgkjzv0+dh8HTtp7V/voiFAJr0vPKFo5WuFU5FPdyL+Kk7bQEB7vj934tytpc9z12F3U0v6GMNkBshbRZxFL56aKWuI5et0tLW82SqNY/zsrAbcD1Y0PYJIFEwli8T6yJ4REyyLY7+dahnbAhF8dWtbkoL20WULK8QJ6Uuzb2JpISoERHKTZbzAS7UsCNjO9S48kjdIqFglFENG4VMqsaRs034egKMzNG5eqw8TvKTThjwG9r4z1c5lGrOaWmnOU8VY6+QBgXP8ICM/A3Xs3GpernP7WROENsDfIfeViO+zSTHvZQma01OVLYG/3SCKgzkNjCWWJherbwKnFgoB6KfhDDx1YmiBjqlAe/WAf1fzv5kZR0ne0dY/y0slvKPHRHbPSRQnJ0e3qFKI4M89gS3/G0oYPqYA0j6MYTGwQsfDQqubBADhyvP4ZFSCDJLc9xkPIc4ZUEWLqnAdQGabZY097GmW6MBh7LrljUcC3HiwOEkHaq387fH/xyKD8Gyc82XzvoDolg1vjbaslVnbFDWWDRPMP2tceHVDyYC7MzFW0OI+YRh3SkzZ9AIBVwR8A==")),
		// )
		if err != nil {
			return nil, err
		}
		s3Client = s3.NewFromConfig(cfg)
		s3Clients[region] = s3Client
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
