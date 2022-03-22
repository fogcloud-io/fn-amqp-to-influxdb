package function

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
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
