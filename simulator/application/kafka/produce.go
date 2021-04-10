package kafka

import (
	"os"
	"log"
	"time"
	"encoding/json"
	"github.com/thienry/fullcycle-projects/tree/main/simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	router "github.com/thienry/fullcycle-projects/tree/main/simulator/application/route"
)

func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := router.NewRoute()
	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()

	positions, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 500)
	}  
}
