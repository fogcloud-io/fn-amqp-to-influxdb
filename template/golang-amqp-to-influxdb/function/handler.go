package function

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/tidwall/gjson"
)

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
