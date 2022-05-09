package main

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/streadway/amqp"
	"github.com/tidwall/gjson"
)

var (
	AMQP_HOST         = os.Getenv("AMQP_HOST")
	AMQP_PORT         = os.Getenv("AMQP_PORT")
	FOG_ACCESS_KEY    = os.Getenv("FOG_ACCESS_KEY")
	FOG_ACCESS_SECRET = os.Getenv("FOG_ACCESS_SECRET")
	ClientId          = "faas"

	onceAmqp          sync.Once
	defaultAMQPClient *RabbitmqClient
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
	initDefaultRabbitmqClient()
	go consumeAMQPData(context.Background())
}

func Handle(w http.ResponseWriter, r *http.Request) {
	var input []byte

	if r.Body != nil {
		defer r.Body.Close()

		body, _ := io.ReadAll(r.Body)

		input = body
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Body: %s", string(input))))
}

func consumeAMQPData(ctx context.Context) {
	ch, err := defaultAMQPClient.Consume(100, FOG_ACCESS_KEY, ClientId)
	if err != nil {
		log.Printf("Consume: %s", err)
		return
	}
	log.Println("amqp consuming...")
	for msg := range ch {
		go handleAMQPData(msg.Body)
	}

	select {
	case <-ctx.Done():
		return
	default:
		go consumeAMQPData(ctx)
	}
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

func getAMQPURLFromEnv() string {
	username, password := getAMQPAccess(FOG_ACCESS_KEY, FOG_ACCESS_SECRET)
	return fmt.Sprintf("amqp://%s:%s@%s:%s", username, password, AMQP_HOST, AMQP_PORT)
}

func initDefaultRabbitmqClient() *RabbitmqClient {
	onceAmqp.Do(func() {
		reconnCtx, cancel := context.WithCancel(context.Background())
		defaultAMQPClient = &RabbitmqClient{
			url:          getAMQPURLFromEnv(),
			cancelReconn: cancel,
			reconnCtx:    reconnCtx,
			closeCh:      make(chan struct{}),
		}
		log.Println("amqp connecting")
		err := defaultAMQPClient.InitConn()
		if err != nil {
			log.Fatalf("amqp init error: %s", err)
		}
	})
	log.Println("amqp connected")
	return defaultAMQPClient
}

type RabbitmqClient struct {
	url           string
	conn          *amqp.Connection
	mqCh          *amqp.Channel
	reConnFlag    uint32
	currReConnNum int

	mu           sync.Mutex
	closeCh      chan struct{}
	cancelReconn context.CancelFunc
	reconnCtx    context.Context
}

func (rc *RabbitmqClient) InitConn() (err error) {
	rc.conn, err = amqp.Dial(rc.url)
	if err != nil {
		return
	}
	rc.mqCh, err = rc.conn.Channel()
	return
}

func (rc *RabbitmqClient) reConn() error {
	var err error
	rc.mu.Lock()
	if !atomic.CompareAndSwapUint32(&rc.reConnFlag, 0, 1) {
		rc.mu.Unlock()
		<-rc.reconnCtx.Done()
		return nil
	}
	rc.reconnCtx, rc.cancelReconn = context.WithCancel(context.Background())
	rc.mu.Unlock()
	for {
		if rc.conn == nil || rc.conn.IsClosed() {
			log.Printf("amqp %dth reconnecting ...", rc.currReConnNum+1)
			err = rc.InitConn()
			if err != nil {
				rc.currReConnNum++
				time.Sleep(time.Second * 10)
				continue
			}
		}
		rc.currReConnNum = 0
		log.Println("amqp reconnected successfully")
		atomic.StoreUint32(&rc.reConnFlag, 0)
		rc.cancelReconn()
		return nil
	}
}

func (rc *RabbitmqClient) Consume(prefetchCnt int, queue, consumerName string) (ch <-chan amqp.Delivery, err error) {
	err = rc.mqCh.Qos(prefetchCnt, 0, false)
	if err != nil {
		err = rc.reConn()
	}
	if err != nil {
		return
	}
	ch, err = rc.mqCh.Consume(queue, consumerName, true, false, false, true, amqp.Table{})
	if err != nil {
		err = rc.reConn()
	}
	return
}

func (rc *RabbitmqClient) Close() error {
	close(rc.closeCh)
	if rc.conn != nil {
		return rc.conn.Close()
	} else {
		return nil
	}
}

func getAMQPAccess(key, secret string) (username, password string) {
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	sign, _ := authAMQPSign(ClientId, key, timestamp, secret, "hmacsha1")
	username = fmt.Sprintf("%s&%s&%s", ClientId, key, timestamp)
	password = sign
	return
}

func authAMQPSign(clientId, accessKey, timestamp, accessSecret, signMethod string) (string, error) {
	src := ""
	src = fmt.Sprintf("clientId%saccessKey%s", clientId, accessKey)
	if timestamp != "" {
		src = src + "timestamp" + timestamp
	}

	var h hash.Hash
	switch signMethod {
	case "hmacsha1":
		h = hmac.New(sha1.New, []byte(accessSecret))
	case "hmacmd5":
		h = hmac.New(md5.New, []byte(accessSecret))
	default:
		return "", errors.New("no access")
	}

	_, err := h.Write([]byte(src))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
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
