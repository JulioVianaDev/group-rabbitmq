package grouprabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func assertBaseTopology(ch *amqp.Channel, cfg *Config) error {
	if err := ch.ExchangeDeclare(
		cfg.ExchangeName,
		cfg.ExchangeType,
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		return fmt.Errorf("exchange %s: %w", cfg.ExchangeName, err)
	}
	if err := ch.ExchangeDeclare(
		cfg.DLXExchangeName,
		"fanout",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("dlx exchange: %w", err)
	}
	deadQ := cfg.QueuePrefix + ".dead"
	if _, err := ch.QueueDeclare(
		deadQ,
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("dead queue: %w", err)
	}
	if err := ch.QueueBind(deadQ, "", cfg.DLXExchangeName, false, nil); err != nil {
		return fmt.Errorf("bind dead queue: %w", err)
	}
	return nil
}

func assertGroupQueue(ch *amqp.Channel, cfg *Config, groupID string) error {
	queueName := cfg.QueuePrefix + "." + groupID
	args := amqp.Table{
		"x-single-active-consumer": true,
		"x-dead-letter-exchange":   cfg.DLXExchangeName,
	}
	if cfg.MessageTTLMs > 0 {
		args["x-message-ttl"] = cfg.MessageTTLMs
	}
	if _, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		args,
	); err != nil {
		return fmt.Errorf("group queue %s: %w", queueName, err)
	}
	if err := ch.QueueBind(queueName, groupID, cfg.ExchangeName, false, nil); err != nil {
		return fmt.Errorf("bind group queue: %w", err)
	}
	return nil
}
