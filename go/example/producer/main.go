// Example producer: publishes the same message shape as node/example/producer.ts
// so Node workers can consume with the existing library.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/group-rabbitmq/group-rabbitmq-go/grouprabbitmq"
)

func main() {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@127.0.0.1:5672"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379"
	}
	queuePrefix := os.Getenv("QUEUE_PREFIX")
	if queuePrefix == "" {
		queuePrefix = "example"
	}

	ctx := context.Background()

	p, err := grouprabbitmq.NewPublisher(grouprabbitmq.Config{
		AMQPURL:     amqpURL,
		RedisURL:    redisURL,
		QueuePrefix: queuePrefix,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := p.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	groups := []string{"group-1", "group-2", "group-3", "group-4", "group-5"}
	for _, groupID := range groups {
		for i := 1; i <= 3; i++ {
			payload := map[string]interface{}{
				"value": fmt.Sprintf("%s-message-%d", groupID, i),
				"group": groupID,
				"index": i,
			}
			msg, err := p.Publish(ctx, groupID, payload, nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("PUBLISHED group=%s seq=%d value=%s\n", groupID, msg.Sequence, payload["value"])
		}
	}
	fmt.Println("Producer done: sent 5 groups x 3 messages.")
}
