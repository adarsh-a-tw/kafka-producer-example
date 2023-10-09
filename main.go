package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	topic := "user-tracking"
	messageGenerator := newRandomMessageGenerator()
	producerCount := 5

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	wg.Add(producerCount)

	exitChan := make(chan any, producerCount)

	go func() {
		for range done {
			log.Println("Signal received to stop producers")
			for i := 0; i < producerCount; i++ {
				exitChan <- struct{}{}
			}
		}
	}()

	for i := 0; i < producerCount; i++ {
		go func() {
			defer wg.Done()

			cfg := sarama.NewConfig()
			cfg.Producer.Partitioner = sarama.NewRandomPartitioner
			cfg.Producer.RequiredAcks = sarama.WaitForAll
			cfg.Producer.Return.Successes = true

			producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, cfg)
			if err != nil {
				panic(err)
			}

			defer producer.Close()

			for {
				select {
				case <-exitChan:
					return
				default:
					payload, _ := json.Marshal(messageGenerator.generate())
					msg := &sarama.ProducerMessage{
						Topic: topic,
						Key:   nil,
						Value: sarama.StringEncoder(payload),
					}

					partition, offset, err := producer.SendMessage(msg)

					if err != nil {
						panic(err)
					}

					log.Printf(
						"Message sent to topic %s, partition %d, offset %d\n",
						topic,
						partition,
						offset,
					)
					time.Sleep(time.Duration(1) * time.Second)
				}
			}
		}()
	}

	wg.Wait()
}

type device string

const (
	ios     device = "IOS"
	android device = "ANDROID"
	web     device = "WEB"
)

type message struct {
	PageKey   string    `json:"page_key"`
	VisitedAt time.Time `json:"visited_at"`
	UserId    int       `json:"user_id"`
	Device    device    `json:"device"`
}

type randomMessageGenerator struct {
	r *rand.Rand
}

func newRandomMessageGenerator() *randomMessageGenerator {
	r := rand.New(rand.NewSource(time.Now().UnixMilli()))
	return &randomMessageGenerator{r}
}

func (rmg *randomMessageGenerator) generate() *message {
	pageKeys := []string{"home", "register", "login", "dashboard"}
	devices := []device{android, ios, web}

	visitedAt := time.Now()
	userId := rmg.r.Intn(100) + 1
	pageKey := pageKeys[rmg.r.Intn(len(pageKeys))]
	device := devices[rmg.r.Intn(len(devices))]

	return &message{
		PageKey:   pageKey,
		VisitedAt: visitedAt,
		UserId:    userId,
		Device:    device,
	}

}
