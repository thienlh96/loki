package main

import (
	"reflect"
	"testing"
)

func Test_mapip(t *testing.T) {
	type args struct {
		ip string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{{
		name: "vn_ip",
		args: args{
			ip: "118.70.14.128",
		},
		want:   "VN",
		wantErr: false,
	},
		
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MapIPToLocation(tt.args.ip,"./geoip/GeoLite2-Country.mmdb")
			if (err != nil) != tt.wantErr {
				t.Errorf("MapIPToLocation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MapIPToLocation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_searchRegion(t *testing.T) {
	type args struct {
		text string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{{
		name: "us_ip",
		args: args{
			text: `{	"requestId": "d686faae-0286-41bd-a78e-c5dc572c662a",    "IP": "34.202.37.93",    "resourcePath": "/napsosi/{proxy+}",    "requestTime": "19/Oct/2023:05:04:16 +0000",    "status": "200",	"error": "-",	"protocol": "HTTP/1.1",    "responseLength": "134",    "clientId": "2369959f-922d-4166-8044-d006206674f5",    "Username": "b2b_admin",    "Email": "b2b_admin@imt-soft.com",    "Phone": "-",	"OrgIDs": "-",    "PoolID": "-",    "IntegrationLatency": "19"}`,
		},
		want:   "US",
		wantErr: false,
	},
		
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := searchRegionFromIp(tt.args.text)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("searchRegionFromIp() = %v, want %v", got, tt.want)
			}
		})
	}
}
