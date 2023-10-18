package main

import (
	"net"
	"regexp"

	"github.com/oschwald/geoip2-golang"
)

// func test() {
// 	// Replace 'GeoLite2-City.mmdb' with the path to your GeoIP database file.
// 	dbPath := "./GeoLite2-Country.mmdb"

// 	ipAddress := "8.8.8.8" // Replace with the IP address you want to look up.

// 	location, err := MapIPToLocation(ipAddress, dbPath)
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 		return
// 	}

// 	fmt.Printf("IP:   %s\nCountry: %s\n", ipAddress, location.Country)
// }

func MapIPToLocation(ipAddress, dbPath string) (string, error) {
	db, err := geoip2.Open(dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	ip := net.ParseIP(ipAddress)
	record, err := db.Country(ip)
	if err != nil {
		return "", err
	}

	return record.Country.IsoCode, nil
}

func FindIPAddresses(input string) []string {
	ipPattern := `\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`
	re := regexp.MustCompile(ipPattern)
	matches := re.FindAllString(input, -1)
	return matches
}
