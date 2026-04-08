package grouprabbitmq

// Config mirrors node/src GroupRabbitMQ defaults so Go publishers interoperate with Node consumers.
type Config struct {
	AMQPURL string
	RedisURL string

	ExchangeName    string // default: group.exchange
	ExchangeType    string // default: direct
	QueuePrefix     string // default: group — also Redis key namespace
	DLXExchangeName string // default: group.dlx
	// Queue arg x-message-ttl (milliseconds). Zero means use the same default as Node
	// (30 * 60 * 1000). Must match an existing queue or RabbitMQ returns PRECONDITION_FAILED.
	MessageTTLMs int32
}

func (c *Config) applyDefaults() {
	if c.ExchangeName == "" {
		c.ExchangeName = "group.exchange"
	}
	if c.ExchangeType == "" {
		c.ExchangeType = "direct"
	}
	if c.QueuePrefix == "" {
		c.QueuePrefix = "group"
	}
	if c.DLXExchangeName == "" {
		c.DLXExchangeName = "group.dlx"
	}
	if c.MessageTTLMs == 0 {
		c.MessageTTLMs = 30 * 60 * 1000 // node/src/GroupRabbitMQ.ts messageTtl default
	}
}
