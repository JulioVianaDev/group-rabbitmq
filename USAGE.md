# Using group-rabbitmq (Node.js & Go)

This document shows how to **install** and **import** the library in a Node.js or Go project.

- **npm package:** [`group-rabbitmq`](https://www.npmjs.com/package/group-rabbitmq)
- **Go module:** `github.com/JulioVianaDev/group-rabbitmq/go` (import path includes the `grouprabbitmq` package)

Releases in this repo use two git tags from the same version (see root `README.md`): `vX.Y.Z` (npm / general) and `go/vX.Y.Z` (Go `go get`).

---

## Node.js (TypeScript / JavaScript)

### Install

```bash
npm install group-rabbitmq
```

With yarn or pnpm:

```bash
yarn add group-rabbitmq
pnpm add group-rabbitmq
```

### Import

The package exposes the built entry from `dist/`; use the package name in imports:

```ts
import { GroupRabbitMQ } from 'group-rabbitmq';
```

### Minimal example

```ts
import { GroupRabbitMQ } from 'group-rabbitmq';

const mq = new GroupRabbitMQ({
  amqpUrl: 'amqp://guest:guest@127.0.0.1:5672',
  redisUrl: 'redis://127.0.0.1:6379',
});

await mq.connect();

await mq.publish('order-1', { item: 'example', qty: 1 });

await mq.consume(async (payload, ctx) => {
  console.log(ctx.groupId, ctx.sequence, payload);
}, { maxConcurrentGroups: 2 });

await mq.subscribeToGroups(['order-1', 'order-2']);

// … when shutting down
await mq.disconnect();
```

For more options (topology, balancing, discovery), see [`node/README.md`](./node/README.md).

---

## Go

The Go side lives under the `go/` directory in the repository. The **module path** is:

`github.com/JulioVianaDev/group-rabbitmq/go`

Application code usually imports the **`grouprabbitmq`** package.

### Install / add to your module

From your project root (where `go.mod` is):

```bash
go get github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq@latest
```

To pin a **specific release**, use the same version as the npm package and the `go/vX.Y.Z` tag (example for `v1.0.1`):

```bash
go get github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq@v1.0.1
```

Go resolves that version using the **`go/v1.0.1`** git tag on the repository.

### Import

```go
import "github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq"
```

### Minimal example (publisher)

The Go module currently provides a **Publisher** that matches the wire format used by the Node library (same exchange, queues, Redis sequence keys, and JSON envelope). Node workers can consume messages published from Go without protocol changes.

```go
package main

import (
	"context"
	"log"

	"github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq"
)

func main() {
	ctx := context.Background()

	p, err := grouprabbitmq.NewPublisher(grouprabbitmq.Config{
		AMQPURL:     "amqp://guest:guest@127.0.0.1:5672",
		RedisURL:    "redis://127.0.0.1:6379",
		QueuePrefix: "example", // must match your Node workers’ queuePrefix
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := p.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	_, err = p.Publish(ctx, "my-group", map[string]any{"k": "v"}, nil)
	if err != nil {
		log.Fatal(err)
	}
}
```

Details, env vars, and alignment with Node defaults are in [`go/README.md`](./go/README.md).

### Local development (fork or clone)

If you need to point at a local copy of this repo instead of the GitHub module:

```go
replace github.com/JulioVianaDev/group-rabbitmq/go => ../path/to/group-rabbitmq/go
```

Adjust the path to your checkout. Run `go mod tidy` after editing `go.mod`.

---

## Summary

| Environment | Install | Import |
|-------------|---------|--------|
| **Node.js** | `npm install group-rabbitmq` | `import { GroupRabbitMQ } from 'group-rabbitmq'` |
| **Go** | `go get github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq@latest` | `import "github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq"` |
