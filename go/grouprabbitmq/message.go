package grouprabbitmq

import "encoding/json"

// GroupMessage is the JSON envelope published to RabbitMQ (matches node/src/types GroupMessage).
type GroupMessage struct {
	GroupID     string          `json:"groupId"`
	MessageID   string          `json:"messageId"`
	Sequence    int64           `json:"sequence"`
	PublishedAt string          `json:"publishedAt"`
	Payload     json.RawMessage `json:"payload"`
}
