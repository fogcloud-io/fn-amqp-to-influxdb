package function

import (
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

var (
	INFLUXDB_HOST     = os.Getenv("INFLUXDB_HOST")
	INFLUXDB_PORT     = os.Getenv("INFLUXDB_PORT")
	INFLUXDB_USER     = os.Getenv("INFLUXDB_USER")
	INFLUXDB_PASSWORD = os.Getenv("INFLUXDB_PASSWORD")
	INFLUXDB_DATABASE = os.Getenv("INFLUXDB_DATABASE")
	INFLUXDB_ENDPOINT = os.Getenv("INFLUXDB_ENDPOINT")
	INFLUXDB_TOKEN    = os.Getenv("INFLUXDB_TOKEN")
	INFLUXDB_ORG      = os.Getenv("INFLUXDB_ORG")
	INFLUXDB_BUCKET   = os.Getenv("INFLUXDB_BUCKET")

	defaultInfluxORG    = "fogcloud-org"
	defaultInfluxBucket = "fogcloud-bucket"

	influxClient influxdb2.Client
	influxWriter api.WriteAPI
)

func initDefaultInfluxDB() {
	influxClient = influxdb2.NewClient(getInfluxEndpointFromEnv(), INFLUXDB_TOKEN)
	influxWriter = influxClient.WriteAPI(getInfluxOrg(), getInfluxBucket())
}

func writeData(measurement string, tags map[string]string, fields map[string]interface{}) {
	p := influxdb2.NewPoint(measurement, tags, fields, time.Now())
	influxWriter.WritePoint(p)
	influxWriter.Flush()
}

func getInfluxEndpointFromEnv() string {
	return INFLUXDB_ENDPOINT
}

func getInfluxOrg() string {
	if INFLUXDB_ORG == "" {
		return defaultInfluxORG
	} else {
		return INFLUXDB_ORG
	}
}

func getInfluxBucket() string {
	if INFLUXDB_BUCKET == "" {
		return defaultInfluxBucket
	} else {
		return INFLUXDB_BUCKET
	}
}
