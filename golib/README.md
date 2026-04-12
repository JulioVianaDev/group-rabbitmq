# group-rabbitmq (Go)

Go **publisher** side that is **wire-compatible** with the Node library under `node/`:

- Same **Redis** sequence key: `{queuePrefix}:group:{groupId}:sequence` (`INCR`)
- Same **RabbitMQ** topology: direct exchange, per-group queue with `x-single-active-consumer`, DLX binding
- Same **JSON body** on the wire:

```json
{
  "groupId": "order-1",
  "messageId": "uuid",
  "sequence": 1,
  "publishedAt": "2026-04-08T12:00:00.000000000Z",
  "payload": { }
}
```

Use this package to **publish from Go** and **consume with Node workers** (`node/example/worker.ts` or your own `GroupRabbitMQ.consume`).

## Requirements

- Go 1.21+
- RabbitMQ + Redis (same as Node). From repo root: `docker compose up -d`
- **Same** `queuePrefix`, `exchangeName`, `dlxExchangeName`, and **queue `x-message-ttl`** as Node (default **30 minutes**). If queues were created by Node first, Go must declare the same arguments or RabbitMQ returns `PRECONDITION_FAILED`.

## Usage

```go
p, err := grouprabbitmq.NewPublisher(grouprabbitmq.Config{
    AMQPURL:     "amqp://guest:guest@127.0.0.1:5672",
    RedisURL:    "redis://127.0.0.1:6379",
    QueuePrefix: "example", // must match Node
})
if err != nil { log.Fatal(err) }
if err := p.Connect(context.Background()); err != nil { log.Fatal(err) }
defer p.Close()

_, err = p.Publish(ctx, "my-group", map[string]any{"k": "v"}, nil)
```

## Example

```bash
cd go
go run ./example/producer
```

Run Node workers with the same `QUEUE_PREFIX` / `queuePrefix` (e.g. `example`).

## Scope

This module currently provides a **Publisher** only. Full consumer parity in Go would mirror `GroupConsumer` + Redis locks; open an issue if you need it.

## Module path

```
github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq
```

Install a tagged version (tags look like `go/v1.0.0`):

```bash
go get github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq@v1.0.0
```

For local development before publishing, use a `replace` directive in your `go.mod`:

```go
replace github.com/JulioVianaDev/group-rabbitmq/go => ../path/to/repo/go
```
