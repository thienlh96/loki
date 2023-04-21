package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/prometheus/common/model"
)

const (
	// We use snappy-encoded protobufs over http by default.
	contentType = "application/x-protobuf"

	maxErrMsgLen = 1024

	invalidExtraLabelsError = "Invalid value for environment variable EXTRA_LABELS. Expected a comma separated list with an even number of entries. "
)

var (
	writeAddress                                              *url.URL
	username, password, extraLabelsRaw, tenantID, bearerToken string
	keepStream                                                bool
	batchSize                                                 int
	s3Clients                                                 map[string]*s3.Client
	extraLabels                                               model.LabelSet
	skipTlsVerify                                             bool
	printLogLine                                              bool
)

func setupArguments() {
	addr := os.Getenv("WRITE_ADDRESS")
	if addr == "" {
		panic(errors.New("required environmental variable WRITE_ADDRESS not present, format: https://<hostname>/loki/api/v1/push"))
	}

	var err error
	writeAddress, err = url.Parse(addr)
	if err != nil {
		panic(err)
	}

	fmt.Println("write address: ", writeAddress.String())

	omitExtraLabelsPrefix := os.Getenv("OMIT_EXTRA_LABELS_PREFIX")
	extraLabelsRaw = os.Getenv("EXTRA_LABELS")
	extraLabels, err = parseExtraLabels(extraLabelsRaw, strings.EqualFold(omitExtraLabelsPrefix, "true"))
	if err != nil {
		panic(err)
	}

	username = os.Getenv("USERNAME")
	password = os.Getenv("PASSWORD")
	// If either username or password is set then both must be.
	if (username != "" && password == "") || (username == "" && password != "") {
		panic("both username and password must be set if either one is set")
	}

	bearerToken = os.Getenv("BEARER_TOKEN")
	// If username and password are set, bearer token is not allowed
	if username != "" && bearerToken != "" {
		panic("both username and bearerToken are not allowed")
	}

	skipTls := os.Getenv("SKIP_TLS_VERIFY")
	// Anything other than case-insensitive 'true' is treated as 'false'.
	if strings.EqualFold(skipTls, "true") {
		skipTlsVerify = true
	}

	tenantID = os.Getenv("TENANT_ID")

	keep := os.Getenv("KEEP_STREAM")
	// Anything other than case-insensitive 'true' is treated as 'false'.
	if strings.EqualFold(keep, "true") {
		keepStream = true
	}
	fmt.Println("keep stream: ", keepStream)

	batch := os.Getenv("BATCH_SIZE")
	batchSize = 131072
	if batch != "" {
		batchSize, _ = strconv.Atoi(batch)
	}

	print := os.Getenv("PRINT_LOG_LINE")
	printLogLine = true
	if strings.EqualFold(print, "false") {
		printLogLine = false
	}
	s3Clients = make(map[string]*s3.Client)
}

func parseExtraLabels(extraLabelsRaw string, omitPrefix bool) (model.LabelSet, error) {
	prefix := "__extra_"
	if omitPrefix {
		prefix = ""
	}
	var extractedLabels = model.LabelSet{}
	extraLabelsSplit := strings.Split(extraLabelsRaw, ",")

	if len(extraLabelsRaw) < 1 {
		return extractedLabels, nil
	}

	if len(extraLabelsSplit)%2 != 0 {
		return nil, fmt.Errorf(invalidExtraLabelsError)
	}
	for i := 0; i < len(extraLabelsSplit); i += 2 {
		extractedLabels[model.LabelName(prefix+extraLabelsSplit[i])] = model.LabelValue(extraLabelsSplit[i+1])
	}
	err := extractedLabels.Validate()
	if err != nil {
		return nil, err
	}
	fmt.Println("extra labels:", extractedLabels)
	return extractedLabels, nil
}

func applyExtraLabels(labels model.LabelSet) model.LabelSet {
	return labels.Merge(extraLabels)
}

func checkEventType(ev map[string]interface{}) (interface{}, error) {
	var s3Event events.S3Event
	var s3TestEvent events.S3TestEvent
	var cwEvent events.CloudwatchLogsEvent
	var kinesisEvent events.KinesisEvent
	var sqsEvent events.SQSEvent

	types := [...]interface{}{&s3Event, &s3TestEvent, &cwEvent, &kinesisEvent, &sqsEvent}

	j, _ := json.Marshal(ev)
	reader := strings.NewReader(string(j))
	d := json.NewDecoder(reader)
	d.DisallowUnknownFields()

	for _, t := range types {
		err := d.Decode(t)

		if err == nil {
			return t, nil
		}

		reader.Seek(0, 0)
	}

	return nil, fmt.Errorf("unknown event type!")
}

func handler(ctx context.Context, ev map[string]interface{}) error {
	lvl, ok := os.LookupEnv("LOG_LEVEL")
	if !ok {
		lvl = "info"
	}
	log := NewLogger(lvl)
	pClient := NewPromtailClient(&promtailClientConfig{
		backoff: &backoff.Config{
			MinBackoff: minBackoff,
			MaxBackoff: maxBackoff,
			MaxRetries: maxRetries,
		},
		http: &httpClientConfig{
			timeout:       timeout,
			skipTlsVerify: skipTlsVerify,
		},
	}, log)

	event, err := checkEventType(ev)
	if err != nil {
		level.Error(*pClient.log).Log("err", fmt.Errorf("invalid event: %s\n", ev))
		return err
	}

	switch evt := event.(type) {
	case *events.S3Event:
		return processS3Event(ctx, evt, pClient, pClient.log)
	case *events.CloudwatchLogsEvent:
		return processCWEvent(ctx, evt, pClient)
	case *events.KinesisEvent:
		return processKinesisEvent(ctx, evt, pClient)
	case *events.SQSEvent:
		return processSQSEvent(ctx, evt)
	// When setting up S3 Notification on a bucket, a test event is first sent, see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
	case *events.S3TestEvent:
		return nil
	}
	return err
}

func main() {
	
	// os.Setenv("EXTRA_LABELS", "env,stag,namespace,aws-service")
	// os.Setenv("LOG_LEVELLOG_LEVEL", "debug")
	// os.Setenv("USERNAME", "")
	// os.Setenv("OMIT_EXTRA_LABELS_PREFIX", "true")
	// os.Setenv("SKIP_TLS_VERIFY", "true")
	// os.Setenv("TENANT_ID", "	torus-tenant")
	// os.Setenv("WRITE_ADDRESS", "http://gateway.loki.torusai.internal:88/loki/api/v1/push")

	setupArguments()

	// evStr := `{
	// 	"Records": [
	// 	  {
	// 		"eventVersion": "2.1",
	// 		"eventSource": "aws:s3",
	// 		"awsRegion": "us-east-1",
	// 		"eventTime": "2023-03-10T10:09:26.809Z",
	// 		"eventName": "ObjectCreated:Put",
	// 		"userIdentity": {
	// 		  "principalId": "AWS:AROAYZ3IT4J6OSW5LUHQT:AWSFirehoseToS3"
	// 		},
	// 		"requestParameters": {
	// 		  "sourceIPAddress": "44.196.47.96"
	// 		},
	// 		"responseElements": {
	// 		  "x-amz-request-id": "Y4SA1BGWVTADQ8G3",
	// 		  "x-amz-id-2": "6kbpmJFBlnEUdn3sh5DZRbMayOOj4cBnYtS99I8fJOJ0tSf4LnMxey3qDt/qCdrRcXr35dbqiZbuqBoyUXjoVBPbenIznewp"
	// 		},
	// 		"s3": {
	// 		  "s3SchemaVersion": "1.0",
	// 		  "configurationId": "a34939c2-38a7-46d7-8476-953a5a11d66c",
	// 		  "bucket": {
	// 			"name": "aws-waf-logs-belletorus-wl-stag-log-bucket",
	// 			"ownerIdentity": {
	// 			  "principalId": "A240Y4IHQ1Z4LV"
	// 			},
	// 			"arn": "arn:aws:s3:::aws-waf-logs-belletorus-wl-stag-log-bucket"
	// 		  },
	// 		  "object": {
	// 			"key": "AWSLogs/year%3D2023/month%3D03/day%3D10/hour%3D10/aws-waf-logs-belletorus-wl-stag-log-deliver-stream-2-2023-03-10-10-08-26-1cdd9c1c-cc65-464d-bc26-0de894e5367d",
	// 			"size": 20386,
	// 			"eTag": "a8c393ea387d14aee162b92b8b4d9b65",
	// 			"versionId": "s9vCG4iYzaHfd.VfAjIQZOkN.c5HhXE9",
	// 			"sequencer": "00640B01D6B6B50A79"
	// 		  }
	// 		}
	// 	  }
	// 	]
	//   }`
	// var ev map[string]interface{}
	// err := json.Unmarshal([]byte(evStr), &ev)
	// if err != nil {
	// }
	// ctx := context.Background()
	// handler(ctx, ev)

	lambda.Start(handler)
}
