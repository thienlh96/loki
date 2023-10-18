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
