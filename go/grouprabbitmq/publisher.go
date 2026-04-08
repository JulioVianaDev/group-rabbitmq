package grouprabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// Publisher publishes messages compatible with the Node group-rabbitmq consumer:
// same exchange, routing key = groupId, body = JSON GroupMessage, sequence from Redis INCR.
type Publisher struct {
	cfg *Config

	redis   *redis.Client
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewPublisher validates config; call Connect before Publish.
func NewPublisher(cfg Config) (*Publisher, error) {
	if cfg.AMQPURL == "" {
		return nil, fmt.Errorf("AMQPURL is required")
	}
	if cfg.RedisURL == "" {
		return nil, fmt.Errorf("RedisURL is required")
	}
	cfg.applyDefaults()
	return &Publisher{cfg: &cfg}, nil
}

// Connect opens Redis, AMQP, a channel, and asserts base topology.
func (p *Publisher) Connect(ctx context.Context) error {
	opt, err := redis.ParseURL(p.cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("redis url: %w", err)
	}
	p.redis = redis.NewClient(opt)
	if err := p.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	conn, err := amqp.Dial(p.cfg.AMQPURL)
	if err != nil {
		_ = p.redis.Close()
		p.redis = nil
		return fmt.Errorf("amqp dial: %w", err)
	}
	p.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		_ = p.redis.Close()
		p.conn = nil
		p.redis = nil
		return fmt.Errorf("amqp channel: %w", err)
	}
	p.channel = ch

	if err := assertBaseTopology(ch, p.cfg); err != nil {
		_ = p.Close()
		return err
	}
	return nil
}

// PublishOptions optional publish flags.
type PublishOptions struct {
	MessageID   string
	Persistent  bool
	ContentType string
}

// Publish marshals payload to JSON, allocates sequence in Redis, and publishes to the direct exchange.
func (p *Publisher) Publish(ctx context.Context, groupID string, payload interface{}, opts *PublishOptions) (*GroupMessage, error) {
	if p.channel == nil {
		return nil, fmt.Errorf("not connected")
	}
	if err := assertGroupQueue(p.channel, p.cfg, groupID); err != nil {
		return nil, err
	}

	seq, err := p.redis.Incr(ctx, sequenceKey(p.cfg.QueuePrefix, groupID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis incr sequence: %w", err)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	msgID := uuid.New().String()
	if opts != nil && opts.MessageID != "" {
		msgID = opts.MessageID
	}

	publishedAt := time.Now().UTC().Format(time.RFC3339Nano)

	env := GroupMessage{
		GroupID:     groupID,
		MessageID:   msgID,
		Sequence:    seq,
		PublishedAt: publishedAt,
		Payload:     payloadBytes,
	}

	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	deliveryMode := amqp.Transient
	persistent := true
	if opts != nil {
		persistent = opts.Persistent
	}
	if persistent {
		deliveryMode = amqp.Persistent
	}

	contentType := "application/json"
	if opts != nil && opts.ContentType != "" {
		contentType = opts.ContentType
	}

	err = p.channel.PublishWithContext(ctx,
		p.cfg.ExchangeName,
		groupID,
		false, false,
		amqp.Publishing{
			ContentType:  contentType,
			DeliveryMode: deliveryMode,
			MessageId:    msgID,
			Body:         body,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("amqp publish: %w", err)
	}

	return &env, nil
}

// Close closes the AMQP channel and connection and Redis.
func (p *Publisher) Close() error {
	var errs []error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			errs = append(errs, err)
		}
		p.channel = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	if p.redis != nil {
		if err := p.redis.Close(); err != nil {
			errs = append(errs, err)
		}
		p.redis = nil
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
