package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	consumer "github.com/fogcloud-io/fog-amqp-consumer"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/tidwall/gjson"
)

var (
	AMQP_HOST         = os.Getenv("AMQP_HOST")
	AMQP_PORT         = os.Getenv("AMQP_PORT")
	FOG_ACCESS_KEY    = os.Getenv("FOG_ACCESS_KEY")
	FOG_ACCESS_SECRET = os.Getenv("FOG_ACCESS_SECRET")
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

func init() {
	initDefaultInfluxDB()
	amqpCli := consumer.InitAMQPConsumer(context.Background(), AMQP_HOST, AMQP_PORT, FOG_ACCESS_KEY, FOG_ACCESS_SECRET)
	go amqpCli.ConsumeWithHanlder(context.Background(), handleAMQPData)
}

func Handler(w http.ResponseWriter, r *http.Request) {
	var input []byte

	if r.Body != nil {
		defer r.Body.Close()

		body, _ := io.ReadAll(r.Body)

		input = body
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Body: %s", string(input))))
}

func handleAMQPData(msg []byte) {
	log.Printf("amqp msg: %s", msg)
	parsedData := gjson.ParseBytes(msg)
	tags := map[string]string{
		"device_id":   parsedData.Get("device_id").String(),
		"product_key": parsedData.Get("product_key").String(),
		"biz_code":    parsedData.Get("biz_code").String(),
	}
	fields := map[string]interface{}{
		"raw_data": parsedData.Get("data").Raw,
	}
	writeData("fogcloud", tags, fields)
}

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
