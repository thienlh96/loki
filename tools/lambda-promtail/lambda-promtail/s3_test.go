package main

import (
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/stretchr/testify/require"
)

func Test_getLabels(t *testing.T) {
	type args struct {
		record events.S3EventRecord
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{{
		name: "s3_rds_audit",
		args: args{
			record: events.S3EventRecord{
				AWSRegion: "us-east-1",
				S3: events.S3Entity{
					Bucket: events.S3Bucket{
						Name: "rds_logs_test",
						OwnerIdentity: events.S3UserIdentity{
							PrincipalID: "test",
						},
					},
					Object: events.S3Object{
						Key: "mothership-instance-1/general_log/year=2023/month=04/day=07/hour=08/1680857100495.log",
					},
				},
			},
		},
		want: map[string]string{
			"bucket":        "rds_logs_test",
			"bucket_owner":  "test",
			"bucket_region": "us-east-1",
			"day":           "07",
			"key":           "mothership-instance-1/general_log/year=2023/month=04/day=07/hour=08/1680857100495.log",
			"month":         "04",
			"log_type":      "general_log",
			"src":           "rds_logs_test",
			"type":          "rds",
			"year":          "2023",
			"hour":          "08",
			"rds_instance":  "mothership-instance-1",
		},
		wantErr: false,
	},
		{
			name: "s3_waf",
			args: args{
				record: events.S3EventRecord{
					AWSRegion: "us-east-1",
					S3: events.S3Entity{
						Bucket: events.S3Bucket{
							Name: "rds_logs_test",
							OwnerIdentity: events.S3UserIdentity{
								PrincipalID: "test",
							},
						},
						Object: events.S3Object{
							Key: "AWSLogs/year=2023/month=03/day=10/hour=03/aws-waf-logs-belletorus-wl-stag-log-deliver-stream-2-2023-03-10-03-20-56-fafab3d8-0af4-4c40-97f2-6cb2914b8a8f",
						},
					},
				},
			},
			want: map[string]string{
				"bucket":        "rds_logs_test",
				"bucket_owner":  "test",
				"bucket_region": "us-east-1",
				"day":           "10",
				"key":           "AWSLogs/year=2023/month=03/day=10/hour=03/aws-waf-logs-belletorus-wl-stag-log-deliver-stream-2-2023-03-10-03-20-56-fafab3d8-0af4-4c40-97f2-6cb2914b8a8f",
				"month":         "03",
				"src":           "aws-waf-logs-belletorus-wl-stag-log-deliver-stream-2-2023-03-10-03-20-56-fafab3d8-0af4-4c40-97f2-6cb2914b8a8f",
				"type":          "waf",
				"year":          "2023",
				"hour":          "03",
			},
			wantErr: false,
		},
		{
			name: "s3_lb",
			args: args{
				record: events.S3EventRecord{
					AWSRegion: "us-east-1",
					S3: events.S3Entity{
						Bucket: events.S3Bucket{
							Name: "elb_logs_test",
							OwnerIdentity: events.S3UserIdentity{
								PrincipalID: "test",
							},
						},
						Object: events.S3Object{
							Key: "my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz",
						},
					},
				},
			},
			want: map[string]string{
				"account_id":    "123456789012",
				"bucket":        "elb_logs_test",
				"bucket_owner":  "test",
				"bucket_region": "us-east-1",
				"day":           "24",
				"key":           "my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz",
				"month":         "01",
				"region":        "us-east-1",
				"src":           "my-loadbalancer",
				"type":          "elasticloadbalancing",
				"year":          "2022",
			},
			wantErr: false,
		},
		{
			name: "s3_flow_logs",
			args: args{
				record: events.S3EventRecord{
					AWSRegion: "us-east-1",
					S3: events.S3Entity{
						Bucket: events.S3Bucket{
							Name: "elb_logs_test",
							OwnerIdentity: events.S3UserIdentity{
								PrincipalID: "test",
							},
						},
						Object: events.S3Object{
							Key: "my-bucket/AWSLogs/123456789012/vpcflowlogs/us-east-1/2022/01/24/123456789012_vpcflowlogs_us-east-1_fl-1234abcd_20180620T1620Z_fe123456.log.gz",
						},
					},
				},
			},
			want: map[string]string{
				"account_id":    "123456789012",
				"bucket":        "elb_logs_test",
				"bucket_owner":  "test",
				"bucket_region": "us-east-1",
				"day":           "24",
				"key":           "my-bucket/AWSLogs/123456789012/vpcflowlogs/us-east-1/2022/01/24/123456789012_vpcflowlogs_us-east-1_fl-1234abcd_20180620T1620Z_fe123456.log.gz",
				"month":         "01",
				"region":        "us-east-1",
				"src":           "fl-1234abcd",
				"type":          "vpcflowlogs",
				"year":          "2022",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getLabels(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("getLabels() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseS3Log(t *testing.T) {
	type args struct {
		b         *batch
		labels    map[string]string
		obj       io.ReadCloser
		filename  string
		batchSize int
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		expectedStream string
	}{
		{
			name: "waf",
			args: args{
				batchSize: 1024000000, // Set large enough we don't try and send to promtail
				filename:  "../testdata/waf-log-test.log",
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"type":       WAF_LOG_TYPE,
					"src":        "source",
					"account_id": "123456789",
					"key":        "waf-log-test.log",
				},
			},
			expectedStream: "{__aws_log_type=\"s3_waf\", __aws_s3_waf=\"source\", __aws_s3_waf_owner=\"123456789\"}",
			wantErr:        false,
		},
		{
			name: "rds_audit",
			args: args{
				batchSize: 1024000000, // Set large enough we don't try and send to promtail
				filename:  "../testdata/audit.gz",
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"type":       RDS_LOG_TYPE,
					"src":        "source",
					"account_id": "123456789",
					"log_type":   "audit",
					"key":        "audit.gz",
				},
			},
			expectedStream: `{__aws_log_type="s3_rds_audit", __aws_rds_audit="source", __aws_s3_rds_audit_owner="123456789"}`,
			wantErr:        false,
		},
		{
			name: "rds_audit",
			args: args{
				batchSize: 1024000000, // Set large enough we don't try and send to promtail
				filename:  "../testdata/rds_error.gz",
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"type":       RDS_LOG_TYPE,
					"src":        "source",
					"account_id": "123456789",
					"log_type":   "error",
					"key":        "rds_error.gz",
				},
			},
			expectedStream: `{__aws_log_type="s3_rds_error", __aws_rds_error="source", __aws_s3_rds_error_owner="123456789"}`,
			wantErr:        false,
		},
		{
			name: "rds_general",
			args: args{
				batchSize: 1024000000, // Set large enough we don't try and send to promtail
				filename:  "../testdata/rds_general.gz",
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"type":       RDS_LOG_TYPE,
					"src":        "source",
					"account_id": "123456789",
					"log_type":   "general",
					"key":        "rds_general.gz",
				},
			},
			expectedStream: `{__aws_log_type="s3_rds_general", __aws_rds_general="source", __aws_s3_rds_general_owner="123456789"}`,
			wantErr:        false,
		},
		{
			name: "vpcflowlogs",
			args: args{
				batchSize: 1024, // Set large enough we don't try and send to promtail
				filename:  "../testdata/vpcflowlog.log.gz",
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"type":       FLOW_LOG_TYPE,
					"src":        "source",
					"account_id": "123456789",
					"key":        "vpcflowlog.log.gz",
				},
			},
			expectedStream: `{__aws_log_type="s3_vpc_flow", __aws_s3_vpc_flow="source", __aws_s3_vpc_flow_owner="123456789"}`,
			wantErr:        false,
		},
		{
			name: "albaccesslogs",
			args: args{
				batchSize: 1024, // Set large enough we don't try and send to promtail
				filename:  "../testdata/albaccesslog.log.gz",
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"type":       LB_LOG_TYPE,
					"src":        "source",
					"account_id": "123456789",
					"key":        "albaccesslog.log.gz",
				},
			},
			expectedStream: `{__aws_log_type="s3_lb", __aws_s3_lb="source", __aws_s3_lb_owner="123456789"}`,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			batchSize = tt.args.batchSize
			tt.args.obj, err = os.Open(tt.args.filename)
			if err != nil {
				t.Errorf("parseS3Log() failed to open test file: %s - %v", tt.args.filename, err)
			}

			if err := parseS3Log(context.Background(), tt.args.b, tt.args.labels, tt.args.obj); (err != nil) != tt.wantErr {
				t.Errorf("parseS3Log() error = %v, wantErr %v", err, tt.wantErr)
			}

			require.Len(t, tt.args.b.streams, 1)
			stream, ok := tt.args.b.streams[tt.expectedStream]
			require.True(t, ok, "batch does not contain stream: %s", tt.expectedStream)
			require.NotNil(t, stream)
		})
	}
}

func Test_parseCWLog(t *testing.T) {
	type args struct {
		b         *batch
		labels    map[string]string
		obj       io.ReadCloser
		txt  string
		batchSize int
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		want    map[string]string
	}{
		{
			name: "waf",
			args: args{
				batchSize: 1024000000, // Set large enough we don't try and send to promtail
				txt:  `{	"requestId": "248ffb94-20d0-44b4-b448-af2b5a92b3a7",    "IP": "113.185.104.76",    "resourcePath": "/diseases/{proxy+}",    "requestTime": "24/Oct/2023:04:31:33 +0000",    "status": "200",	"error": "-",	"protocol": "HTTP/1.1",    "responseLength": "1036",    "clientId": "-",    "Username": "-",    "Email": "-",    "Phone": "-",	"OrgIDs": "-",    "PoolID": "-",    "IntegrationLatency": "14"}`,
				b: &batch{
					streams: map[string]*logproto.Stream{},
				},
				labels: map[string]string{
					"requestId":       "248ffb94-20d0-44b4-b448-af2b5a92b3a7",
					"IP":        "source",
					"account_id": "123456789",
					"key":        "waf-log-test.log",
				},
			},
			want: map[string]string{
				"requestId": "248ffb94-20d0-44b4-b448-af2b5a92b3a7",
				"IP": "113.185.104.76",
				"resourcePath": "/diseases/{proxy+}",
				"requestTime": "24/Oct/2023:04:31:33 +0000",
				"status": "200",
				"error": "-",
				"protocol": "HTTP/1.1",
				"responseLength": "1036",
				"clientId": "-",
				"Username": "-",
				"Email": "-",
				"Phone": "-",
				"OrgIDs": "-",
				"PoolID": "-",
				"IntegrationLatency": "14",
				"region":"VN",
			},
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// var err error
			batchSize = tt.args.batchSize
			labels := parser_json(  tt.args.txt)
			require.Len(t, labels, 16)
			if !reflect.DeepEqual(labels, tt.want) {
				t.Errorf("getLabels() = %v, want %v", labels, tt.want)
			}
		})
	}
}
