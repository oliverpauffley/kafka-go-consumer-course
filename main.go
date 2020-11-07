package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
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

	numWorkers := runtime.NumCPU()
	flushBytes := 5e+6

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "twitter",
		Client:        es,
		NumWorkers:    numWorkers,
		FlushBytes:    int(flushBytes),
		FlushInterval: 30 * time.Second,
	})
	defer bi.Close(ctx)

	if err != nil {
		log.Fatalf("error creating indexer: %s", err)
	}

	twitterKafkaConsumer := NewKafkaReader(kafkaAdddress, kafkaTopic)
	defer twitterKafkaConsumer.Close()

	for {
		m, err := twitterKafkaConsumer.ReadMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		err = InsertTweet(ctx, bi, string(m.Key), bytes.NewReader(m.Value))
		if err != nil {
			log.Fatal(err)
		}

	}

}

//InsertTweet will insert a tweet into the elastic search bulk index "twitter". This is idempotent as the tweetID is unique.
func InsertTweet(ctx context.Context, bi esutil.BulkIndexer, tweetID string, tweetReader io.Reader) error {
	err := bi.Add(
		ctx,
		esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: tweetID,
			Body:       tweetReader,
			OnFailure: func(c context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("ERROR: %s", err)
				} else {
					log.Printf("ERROR: %s: %s", resp.Error.Type, resp.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		log.Fatalf("error with indexer: %s", err)
	}
	return nil
}

// NewKafkaReader returns a new kafka io.Reader that can be used to communicate with a kafka broker.
func NewKafkaReader(kafkaAddress, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{
			kafkaAddress,
		},
		GroupID:  "elastic-search",
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}
