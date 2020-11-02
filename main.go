package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()

	kafkaAdddress, ok := os.LookupEnv("KAFKA_BROKER_ADDRESS")
	if !ok {
		log.Println("kafka broker address no set, using default localhost:9092")
		kafkaAdddress = "localhost:9092"
	}
	kafkaTopic, ok := os.LookupEnv("KAFKA_TOPIC")
	if !ok {
		log.Fatal("must set env var 'KAFKA_TOPIC'")
	}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("error creating es client: %s", err)
	}

	twitterKafkaConsumer := NewKafkaReader(kafkaAdddress, kafkaTopic)
	defer twitterKafkaConsumer.Close()

	for {
		m, err := twitterKafkaConsumer.ReadMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		err = InsertTweet(ctx, es, string(m.Key), bytes.NewReader(m.Value))
		if err != nil {
			log.Fatal(err)
		}

	}

}

//InsertTweet will insert a tweet into the elastic search index "twitter". This is idempotent as the tweetID is unique.
func InsertTweet(ctx context.Context, es *elasticsearch.Client, tweetID string, TweetReader io.Reader) error {
	req := esapi.IndexRequest{
		Index:      "twitter",
		DocumentID: tweetID,
		Body:       TweetReader,
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("error indexing tweet, %s/%s", res.Status(), res.String())
	}

	return nil
}

// NewKafkaReader returns a new kafka io.Reader that can be used to communicate with a kafka broker.
func NewKafkaReader(kafkaAddress, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaAddress},
		GroupID:  "elastic-search",
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}
