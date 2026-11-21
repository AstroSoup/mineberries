# Mineberries

Mineberries sells and delivers Minecraft items. Service A is a Java/Spring Boot
application that owns orders and hands work to bots. Service B is a TypeScript
process that runs the Minecraft bot pool. They exchange messages through RabbitMQ.

## Local stack

The development stack runs PostgreSQL 18 and RabbitMQ with the management plugin.
It creates separate databases and RabbitMQ users for services A and B so the two
sides do not share credentials.

### Prerequisites

- Docker Engine with the Compose plugin
- No local PostgreSQL or RabbitMQ installation is required

### Start

From the repository root:

```bash
cp .env.example .env
docker compose up -d --wait
```

`--wait` returns after both containers report healthy. Check them with:

```bash
docker compose ps
docker compose logs -f postgres rabbitmq
```

The RabbitMQ management UI is at <http://localhost:15672>. Log in with
`RABBITMQ_ADMIN_USER` and `RABBITMQ_ADMIN_PASSWORD` from `.env`.

The example ports are PostgreSQL `5432`, AMQP `5672`, and the management UI
`15672`. Change the port variables in `.env` if they are already in use, and
update the matching `SPRING_*` and `BOT_*` endpoint values as well. The ports are
published on `127.0.0.1` only.

### Credentials and databases

`.env.example` contains local placeholder passwords only. Copying it to `.env`
is required because `.env` is ignored by git and must never contain committed
credentials. Change the placeholders if anyone else can reach your machine.

| Service | Database | Database user | RabbitMQ user |
| --- | --- | --- | --- |
| A | `APP_DB_NAME` | `APP_DB_USER` / `APP_DB_PASSWORD` | `RABBITMQ_APP_USER` / `RABBITMQ_APP_PASSWORD` |
| B | `BOT_DB_NAME` | `BOT_DB_USER` / `BOT_DB_PASSWORD` | `RABBITMQ_BOT_USER` / `RABBITMQ_BOT_PASSWORD` |

Both service users have access to the dedicated vhost in `RABBITMQ_VHOST`. The
`POSTGRES_ADMIN_*` values only bootstrap the PostgreSQL container; services should
not use the administrator account.

### How service A reads configuration

Service A is a Spring Boot application, so it reads standard Spring properties.
Spring's relaxed binding maps the following environment variables from `.env`:

| Environment variable | Spring property |
| --- | --- |
| `SPRING_DATASOURCE_URL` | `spring.datasource.url` |
| `SPRING_DATASOURCE_USERNAME` | `spring.datasource.username` |
| `SPRING_DATASOURCE_PASSWORD` | `spring.datasource.password` |
| `SPRING_RABBITMQ_HOST` | `spring.rabbitmq.host` |
| `SPRING_RABBITMQ_PORT` | `spring.rabbitmq.port` |
| `SPRING_RABBITMQ_USERNAME` | `spring.rabbitmq.username` |
| `SPRING_RABBITMQ_PASSWORD` | `spring.rabbitmq.password` |
| `SPRING_RABBITMQ_VIRTUAL_HOST` | `spring.rabbitmq.virtual-host` |

The `SPRING_*` values point at the app database and app RabbitMQ user. They are
listed explicitly in `.env.example`; keep them consistent with the corresponding
`APP_DB_*` and `RABBITMQ_APP_*` stack values. If you run Spring from your IDE,
export the variables first (for example `set -a; . ./.env; set +a`) or configure
the equivalent run-configuration environment.

### How service B reads configuration

Service B is a TypeScript/Node process and reads environment variables directly
through `process.env`. The local values are:

| Variable | Meaning |
| --- | --- |
| `BOT_DB_HOST`, `BOT_DB_PORT` | PostgreSQL endpoint |
| `BOT_DB_NAME`, `BOT_DB_USER`, `BOT_DB_PASSWORD` | bot database credentials |
| `BOT_RABBITMQ_HOST`, `BOT_RABBITMQ_PORT` | AMQP endpoint |
| `BOT_RABBITMQ_USERNAME`, `BOT_RABBITMQ_PASSWORD` | bot RabbitMQ credentials |
| `BOT_RABBITMQ_VHOST` | RabbitMQ vhost |

Load `.env` when starting Node, for example:

```bash
node --env-file=.env build/libs/dist/main.js
```

Keep the `BOT_*` values consistent with the corresponding `BOT_DB_*` and
`RABBITMQ_BOT_*` stack values.

### RabbitMQ topology

Service A declares these objects on startup in `spring.rabbitmq.virtual-host`
(`SPRING_RABBITMQ_VIRTUAL_HOST`). The vhost must already exist; the local stack creates
it from `RABBITMQ_VHOST`. Keep Service B on the same vhost.

| Default name | Object | Purpose | Service A property |
| --- | --- | --- | --- |
| `tasks` | Durable quorum queue | Carries tasks and cancellations from A to B | `mineberries.rabbitmq.tasks-queue` |
| `events` | Durable quorum queue | Carries task lifecycle events from B to A | `mineberries.rabbitmq.events-queue` |
| `tasks.dlx` | Durable direct exchange | Routes dead letters from `tasks` to `tasks.dlq` | `mineberries.rabbitmq.tasks-dead-letter-exchange` |
| `events.dlx` | Durable direct exchange | Routes dead letters from `events` to `events.dlq` | `mineberries.rabbitmq.events-dead-letter-exchange` |
| `tasks.dlq` | Durable classic queue | Holds dead-lettered tasks and cancellations | `mineberries.rabbitmq.tasks-dead-letter-queue` |
| `events.dlq` | Durable classic queue | Holds dead-lettered events | `mineberries.rabbitmq.events-dead-letter-queue` |

The routing key for each dead-letter binding is its dead-letter queue name.
`mineberries.rabbitmq.delivery-limit` sets the failed-delivery limit on both quorum
queues (default `20`). RabbitMQ dead-letters messages that exceed the limit.
Redeclaration is safe when both services use the same names and arguments. To change
arguments on an existing queue, coordinate a queue migration; restarting with different
arguments does not update the queue in place. See the
[message contract](documentation/message-contract.md#1-transport) for declaration details.

The Service A outbox relay polls with a fixed delay (`mineberries.outbox.relay.interval`,
default `1s`). It locks up to `mineberries.outbox.relay.batch-size` rows at a time
(default `100`) and waits up to `mineberries.outbox.relay.confirm-timeout` for each
publisher confirm (default `5s`).

Failed rows remain unsent and retry independently. `mineberries.outbox.relay.retry-base`
(default `1s`) sets the initial retry ceiling, which doubles per failed attempt up to
`mineberries.outbox.relay.retry-cap` (default `1m`). The actual delay is randomly chosen
between half the ceiling and the ceiling, with a minimum of 1 ms.

Outbox cleanup runs hourly by default. Set `mineberries.outbox.cleanup.cron` to change
its UTC cron schedule (use `-` to disable it), and `mineberries.outbox.cleanup.retention`
to change how long sent rows are kept (default `7d`). Cleanup commits at most 1,000
deletions per transaction and never removes unsent rows.

### Initialization and reset

PostgreSQL runs `docker/postgres/init/10-create-service-databases.sh` only when
its named volume is empty. RabbitMQ renders
`docker/rabbitmq/definitions.template.json` from `.env` and imports the vhost,
users, and permissions only when its named volume is empty.

After changing database names, users, passwords, or RabbitMQ credentials, recreate
the local data:

```bash
docker compose down -v
docker compose up -d --wait
```

`docker compose down` stops the stack but keeps the named volumes. `down -v`
deletes them. Use `down -v` only when you are willing to lose local data.
