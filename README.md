# group-rabbitmq

Monorepo: **Node.js** library in [`node/`](./node/) and **Go** publisher in [`go/`](./go/), wire-compatible for the same RabbitMQ queues and Redis sequence keys.

- **Repository:** [github.com/JulioVianaDev/group-rabbitmq](https://github.com/JulioVianaDev/group-rabbitmq)

## Install

### npm (TypeScript / JavaScript)

```bash
npm install group-rabbitmq
```

Package root is the [`node/`](./node/) folder (`repository.directory` in `package.json`).

### Go

Module path (subfolder `go/`):

```bash
go get github.com/JulioVianaDev/group-rabbitmq/go/grouprabbitmq@latest
```

Releases use the **`go/vX.Y.Z`** git tag (see [Go submodules](https://go.dev/wiki/Modules#how-can-i-use-modules-for-repository-migration-or-to-track-multiple-modules-in-one-repository)).

## Releases & tags

Pushes to **`main`** run [Tag release](.github/workflows/tag-release.yml): it reads **`node/package.json`** `version` and creates:

| Tag | Purpose |
|-----|---------|
| `v1.0.0` | npm / GitHub releases |
| `go/v1.0.0` | `go get` for the module under `go/` |

Bump the version in [`node/package.json`](./node/package.json) when you want a new release; duplicate tags are skipped.

### Publish to npm (manual)

```bash
cd node
npm publish --access public
```

Use an npm [automation token](https://docs.npmjs.com/creating-and-viewing-access-tokens) with `NPM_TOKEN` in GitHub Secrets if you add a publish workflow later.

## Docs

- [Node README](./node/README.md)
- [Go README](./go/README.md)

## Local dev

```bash
docker compose up -d
cd node && npm test
cd go && go test ./...
```
