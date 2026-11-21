# Mineberries message contract

This document is the source of truth for messages exchanged by service A (Java, owns
orders) and service B (TypeScript, runs the Minecraft bots) through RabbitMQ.

The machine-readable contract is in [`documentation/schemas/`](schemas/). The schemas use
JSON Schema draft 2020-12. Examples are in
[`documentation/schemas/examples/`](schemas/examples/).

Changing this contract is a two-sided change: update this document and the schemas first,
then change the Java and TypeScript implementations.

## 1. Transport

Messages are UTF-8 JSON, one JSON object per AMQP message, with
`content_type=application/json`.

| Queue | Direction | Kind | Dead-letter exchange | Dead-letter queue |
| --- | --- | --- | --- | --- |
| `tasks` | A to B | task and cancel messages | `tasks.dlx` | `tasks.dlq` |
| `events` | B to A | task lifecycle events | `events.dlx` | `events.dlq` |

All queues and exchanges in the table are durable and not auto-deleted. The source
queues are non-exclusive quorum queues (`x-queue-type=quorum`); the dead-letter queues
are non-exclusive classic queues (`x-queue-type=classic`). Each source queue sets
`x-dead-letter-exchange` to its exchange, `x-dead-letter-routing-key` to its dead-letter
queue name, and `x-delivery-limit` to the configured failed-delivery limit (default
`20`). Each dead-letter exchange is `direct` and binds to its dead-letter queue using
that queue's name as the routing key. RabbitMQ dead-letters messages that exceed the
limit.

Publishers use persistent messages. Either service can declare the same topology
idempotently at startup. If names or the delivery limit are overridden, both services
must use the same vhost, names, and declaration arguments.

Transport is at-least-once. Any message may be delivered more than once, and messages may
be redelivered after a consumer or broker restart. Correctness comes from stable ids,
idempotent consumers, and the version rules below, not from exactly-once delivery.

Outgoing messages are written to a transactional outbox and published with publisher
confirms. A broker rejection therefore does not lose a committed task, and the same
`messageId` survives every publish retry.

Service A's relay publishes to the configured `tasks` queue through the default
exchange with the AMQP mandatory flag and persistent delivery mode. It copies the
task UUID to the AMQP `messageId` property and the stored `kind` and `type` to the
headers. The body is the complete JSON envelope; for a cancel the `type` header
is `cancel`, but the body still has no `type` field.

Service A writes the complete wire message to `outbox.payload` in the caller's database
transaction. The `message_id`, `kind`, and `type` columns are separate row metadata.
Cancel rows use `cancel` as their stored `type`; the cancel JSON still has no `type`
field. Enqueueing completes when the row is written, not when the transaction commits or
RabbitMQ confirms publication.

When A submits a task, it stores the new task and enqueues its message in the same
business transaction. The task becomes active in memory only after commit. An enqueue
failure aborts the submission transaction; it is not a `FAILED` event from B.

The relay locks due, unsent rows in ID order, skipping rows locked by another relay.
It sets `sent_at` per row only after RabbitMQ confirms that message without returning
it. A nack, timeout, connection failure, or mandatory return leaves that row unsent.
The relay increments its `attempts`, records a payload-free `last_error`, and sets
`next_attempt_at` using capped exponential backoff with jitter. Other due rows can
still be published. Retries and concurrent batches can arrive out of order. If RabbitMQ
accepted a message before its confirmation was lost or the database transaction failed,
a later publish can deliver it again.

Service A retains sent outbox rows for a configurable period (seven days by default).
Cleanup uses `sent_at`, not creation time, and never deletes unsent rows.

Service A validates incoming events against the event schema before invoking its
subscribers. Invalid messages are rejected without requeue and go to `events.dlq`.
Valid events are acknowledged only after the handler's database transaction commits.
If handling or committing fails, A rejects the delivery with requeue so that it
counts toward the quorum queue's delivery limit (RabbitMQ 4.3+ does not count
`basic.nack` with requeue). A defaults to one consumer to preserve delivery order;
raising concurrency permits events for the same task to be handled out of order.

For a valid event, A locks the task row and compares the event version against
the last version it persisted. Unknown tasks, duplicate or older versions, and
events after a terminal state are acknowledged without changing the task row.
Accepted changes are saved in the same transaction, even if A has no in-memory
task for that ID. Local progress listeners and result futures are updated only
after commit.

## 2. Common conventions

- **`messageId`** is a UUID. The producer creates it once and reuses it on every retry or
  redelivery. For events and cancel messages it is the id of the task being reported on
  or cancelled, not a newly generated id.
- **Instants** are UTC ISO-8601 strings in the form produced by
  `java.time.Instant.toString()`, for example `2026-11-19T10:15:30Z`. Fractional seconds
  are optional.
- **Ids** (`orderId`, `warehouseId`, `itemId`) are positive JSON integers.
- **Money** is a positive JSON number with at most two decimal places and no more than
  `9999999999.99`.
- **Unknown fields are ignored by consumers** at every level: envelope fields, payload
  fields, and nested objects. The schemas therefore allow additional properties.
  Producers must not reuse an existing field name for a different meaning.
- **Null values are meaningful.** `itemId` may be `null` for an item that is not in the
  catalog. `dropoffContainer` may be `null` when no drop-off container was used. In both
  cases the field key is still present.
- **Secrets are never logged.** A verification `code` is a one-time secret and must not
  appear in logs, metrics, traces, or error messages.

Shared object shapes:

| Name | Fields |
| --- | --- |
| `WorldPoint` | `world` (non-blank string), `x` (integer), `y` (integer, -64 to 320), `z` (integer) |
| `ItemAmount` | `itemId` (positive integer or `null`), `material` (non-blank string), `quantity` (positive integer) |
| `InventoryDiscrepancy` | `itemId` (positive integer or `null`), `material`, `expectedQuantity` (>= 0), `actualQuantity` (>= 0); the two quantities must differ |

## 3. Task message

Published by A on `tasks`. There are two message kinds on this queue: `task` and `cancel`.

```json
{
  "messageId": "6a7b8c9d-1e2f-4a3b-9c4d-5e6f7a8b9c1d",
  "kind": "task",
  "type": "courier.deliver_order",
  "schemaVersion": 1,
  "payload": {
    "orderId": 42,
    "pickupPoint": { "world": "overworld", "x": 100, "y": 64, "z": -20 },
    "pickupContainer": { "world": "overworld", "x": 105, "y": 64, "z": -20 },
    "dropoffPoint": { "world": "overworld", "x": 200, "y": 70, "z": 50 },
    "dropoffContainer": { "world": "overworld", "x": 199, "y": 70, "z": 50 }
  }
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `messageId` | UUID | yes | Task id, created once by A |
| `kind` | `"task"` | yes | Message kind |
| `type` | string | yes | Task type, see the catalog below |
| `schemaVersion` | integer >= 1 | yes | Version of the payload schema for this task type |
| `expiresAt` | instant | no | Optional deadline; set by A for the task types that have deadlines |
| `payload` | object | yes | Task-specific payload |

### Task catalog

Every task type and its `COMPLETED` result are listed here. The event does not repeat the
task type; A associates the event's `messageId` with the submitted task and therefore
knows which result shape to expect.

| Task type | Task payload | `COMPLETED` payload | Deadline |
| --- | --- | --- | --- |
| `banker.send_verification_code` | `nickname`, `code` | `VerificationCodeDeliveryResult`: `nickname`, `deliveredAt` | 2 minutes |
| `banker.collect_payment` | `payerNickname`, `amount`, `timeoutSeconds` | `PaymentCollectedResult`: `payerNickname`, `amount`, `paidAt` | 15 minutes |
| `banker.send_payment` | `recipientNickname`, `amount` | `PaymentSentResult`: `recipientNickname`, `amount`, `paidAt` | none |
| `supervisor.run_inventory` | `warehouseId`, `expectedItems` | `InventoryReport`: `warehouseId`, `actualItems`, `discrepancies`, `completedAt` | none |
| `supervisor.load_courier_parcel` | `warehouseId`, `parcelPoint`, `items` | `CourierParcelLoadResult`: `parcelPoint`, `loadedItems`, `missingItems`, `loadedAt` | none |
| `supervisor.unload_courier_parcel` | `warehouseId`, `parcelPoint` | `CourierParcelUnloadResult`: `parcelPoint`, `unloadedItems`, `remainingItems`, `unloadedAt` | none |
| `courier.deliver_order` | `orderId`, `pickupPoint`, `pickupContainer`, `dropoffPoint`, `dropoffContainer` | `DeliveryResult`: `orderId`, `outcome`, `dropoffContainer`, `usedFallbackContainer`, `movedItems`, `finishedAt` | none |

Each result class also has a stable Java type id. It is not sent on the wire; the task id
identifies the task and therefore the expected result.

| Task type | Result type id |
| --- | --- |
| `banker.send_verification_code` | `banker.send_verification_code.result` |
| `banker.collect_payment` | `banker.collect_payment.result` |
| `banker.send_payment` | `banker.send_payment.result` |
| `supervisor.run_inventory` | `supervisor.run_inventory.result` |
| `supervisor.load_courier_parcel` | `supervisor.load_courier_parcel.result` |
| `supervisor.unload_courier_parcel` | `supervisor.unload_courier_parcel.result` |
| `courier.deliver_order` | `courier.deliver_order.result` |

Payload details:

- `banker.send_verification_code` carries the player's `nickname` and the one-time
  `code` to deliver. Code generation, purpose, validation, and expiry belong to A.
- `banker.collect_payment` asks `payerNickname` for `amount` and waits at most
  `timeoutSeconds` for the transfer. The business operation behind the payment is tracked
  by A, not carried in the task. The product deadline is 15 minutes, so A sets
  `timeoutSeconds` to `900`.
- `banker.send_payment` pays `amount` to `recipientNickname` from the service account.
  The reason for the payment and related business records are tracked by A.
- `supervisor.run_inventory` scans the warehouse's containers and compares the physical
  contents with `expectedItems`. `expectedItems` may be empty for an empty warehouse.
  The warehouse's container locations are discovered by B from the world and are not
  task inputs.
- `supervisor.load_courier_parcel` loads `items` (non-empty) from warehouse storage into
  the parcel container at `parcelPoint`.
- `supervisor.unload_courier_parcel` empties the parcel at `parcelPoint` into warehouse
  storage.
- `courier.deliver_order` moves everything from `pickupContainer` to a container near
  `dropoffPoint`. The task carries no item list and no player nicknames; the cargo is
  whatever is physically in `pickupContainer`. `dropoffContainer` may be `null` when the
  service does not know a specific target container; the courier reports the container it
  actually used in the result.
- `DeliveryResult.outcome` is one of `DELIVERED`, `DROPOFF_BLOCKED`, or `CARGO_LOST`.
  `DROPOFF_BLOCKED` means neither the requested container nor a fallback container could
  be used. `CARGO_LOST` means the cargo cannot be recovered, for example after a death in
  a place the items cannot leave. `dropoffContainer` is non-null for `DELIVERED` and may
  be `null` otherwise. `movedItems` records the items physically moved, not a judgement
  about order completeness.

### Task examples

| Task type | Example |
| --- | --- |
| `banker.send_verification_code` | [`task.banker.send_verification_code.json`](schemas/examples/task.banker.send_verification_code.json) |
| `banker.collect_payment` | [`task.banker.collect_payment.json`](schemas/examples/task.banker.collect_payment.json) |
| `banker.send_payment` | [`task.banker.send_payment.json`](schemas/examples/task.banker.send_payment.json) |
| `supervisor.run_inventory` | [`task.supervisor.run_inventory.json`](schemas/examples/task.supervisor.run_inventory.json) |
| `supervisor.load_courier_parcel` | [`task.supervisor.load_courier_parcel.json`](schemas/examples/task.supervisor.load_courier_parcel.json) |
| `supervisor.unload_courier_parcel` | [`task.supervisor.unload_courier_parcel.json`](schemas/examples/task.supervisor.unload_courier_parcel.json) |
| `courier.deliver_order` | [`task.courier.deliver_order.json`](schemas/examples/task.courier.deliver_order.json) |

## 4. Cancel message

Published by A on `tasks` when it wants to stop a task. `messageId` is the id of the task
being cancelled; no new task id is created.

```json
{
  "messageId": "6a7b8c9d-1e2f-4a3b-9c4d-5e6f7a8b9c1d",
  "kind": "cancel",
  "schemaVersion": 1
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `messageId` | UUID | yes | Id of the task to cancel |
| `kind` | `"cancel"` | yes | Message kind |
| `schemaVersion` | integer >= 1 | yes | Version of the cancel message schema |

Cancellation is cooperative and idempotent. Duplicate cancel messages have no additional
effect. If the task is already terminal, B ignores the cancel. Otherwise B stops at a safe
point and emits exactly one terminal event: `CANCELED` if it stopped before a side effect,
or `FAILED` with `outcomeUnknown=true` and `retryable=false` if it cannot rule out that a
side effect already happened.

Cancellation may race with completion. The first terminal event that reaches A wins;
later events for the same task are ignored.

See [`cancel.courier.deliver_order.json`](schemas/examples/cancel.courier.deliver_order.json).

## 5. Event message

Published by B on `events`. Events do not carry a `kind` field; the queue identifies them
as events. `messageId` is the task id from the original task message.

```json
{
  "messageId": "6a7b8c9d-1e2f-4a3b-9c4d-5e6f7a8b9c1d",
  "version": 0,
  "type": "ACCEPTED",
  "occurredAt": "2026-11-19T10:15:02Z",
  "payload": {
    "bot": "courier_andy"
  }
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `messageId` | UUID | yes | Id of the task this event belongs to |
| `version` | integer >= 0 | yes | Event version for this task; starts at 0 and strictly increases |
| `type` | string | yes | `ACCEPTED`, `PROGRESS`, `COMPLETED`, `FAILED`, or `CANCELED` |
| `occurredAt` | instant | yes | Time B produced the event |
| `payload` | object | yes | Type-specific payload |

`version` is scoped to one task. A ignores a version it has already applied and any
version lower than the highest one seen. Redelivery of an event reuses the same
`(messageId, version)`, which is the deduplication key.

| Event type | Payload |
| --- | --- |
| `ACCEPTED` | `{ "bot": string }` - the bot that took the task |
| `PROGRESS` | `{ "stage": string }` |
| `COMPLETED` | The result object for the task type, see the catalog above |
| `FAILED` | `{ "message": string, "retryable": boolean, "outcomeUnknown": boolean }` |
| `CANCELED` | `{}` (empty object; unknown fields are ignored) |

`ACCEPTED` is normally the first event and has version 0. `COMPLETED`, `FAILED`, and
`CANCELED` are terminal; exactly one terminal event is produced per task. A task may have
zero or more `PROGRESS` events.

`FAILED` rules:

- `retryable` tells A whether submitting the same work again is safe.
- `outcomeUnknown` means the bot cannot determine whether a side effect happened. When
  `outcomeUnknown` is `true`, `retryable` must be `false`.
- Payment tasks never retry blindly. An unknown payment outcome is reported as
  `FAILED` with `outcomeUnknown=true` and `retryable=false`; A records a payment incident
  and does not retry automatically.

### Result examples

| Result | Example |
| --- | --- |
| `VerificationCodeDeliveryResult` | [`event.completed.banker.send_verification_code.json`](schemas/examples/event.completed.banker.send_verification_code.json) |
| `PaymentCollectedResult` | [`event.completed.banker.collect_payment.json`](schemas/examples/event.completed.banker.collect_payment.json) |
| `PaymentSentResult` | [`event.completed.banker.send_payment.json`](schemas/examples/event.completed.banker.send_payment.json) |
| `InventoryReport` | [`event.completed.supervisor.run_inventory.json`](schemas/examples/event.completed.supervisor.run_inventory.json) |
| `CourierParcelLoadResult` | [`event.completed.supervisor.load_courier_parcel.json`](schemas/examples/event.completed.supervisor.load_courier_parcel.json) |
| `CourierParcelUnloadResult` | [`event.completed.supervisor.unload_courier_parcel.json`](schemas/examples/event.completed.supervisor.unload_courier_parcel.json) |
| `DeliveryResult` | [`event.completed.courier.deliver_order.json`](schemas/examples/event.completed.courier.deliver_order.json) |

Other event examples:

| Event | Example |
| --- | --- |
| `ACCEPTED` | [`event.accepted.courier.deliver_order.json`](schemas/examples/event.accepted.courier.deliver_order.json) |
| `PROGRESS` | [`event.progress.courier.deliver_order.json`](schemas/examples/event.progress.courier.deliver_order.json) |
| `FAILED` with unknown payment outcome | [`event.failed.banker.collect_payment.json`](schemas/examples/event.failed.banker.collect_payment.json) |
| `CANCELED` | [`event.canceled.courier.deliver_order.json`](schemas/examples/event.canceled.courier.deliver_order.json) |

The event examples are illustrative alternative histories, not one continuous event
stream.

## 6. Deadlines

Only two operations have deadlines:

| Operation | Deadline | Result timestamp checked by A |
| --- | --- | --- |
| Verification code delivery | 2 minutes | `deliveredAt` |
| Payment awaited from a player | 15 minutes | `paidAt` |

A includes `expiresAt` on `banker.send_verification_code` and `banker.collect_payment`
task messages. Other task types currently have no deadline and do not carry the field.
For `collect_payment`, A also sets `timeoutSeconds` to `900`.

A checks the deadline against the timestamp inside the result (`deliveredAt` or `paidAt`)
with a large configured buffer. It never uses the moment a `COMPLETED` event arrives as
the basis for the deadline decision. This keeps the business decision independent of
broker delays and redeliveries.

## 7. Schema versions

- `schemaVersion` is an integer starting at `1`.
- Task messages version the payload schema of their task type. A cancel message versions
  the cancel message schema.
- The event envelope has no `schemaVersion`. B selects the result schema using the
  `schemaVersion` of the task that produced the event, so changing a result shape is a
  breaking change to that task type.
- Increment the version for a breaking change: adding a required field, removing or
  renaming a field, changing a field type or meaning, or changing the envelope.
- Adding an optional field that old consumers can ignore does not require a version bump,
  because unknown fields are ignored. A producer may still bump the version when it wants
  explicit feature gating.
- A consumer that sees a `schemaVersion` it does not support must fail safely rather than
  guessing the payload shape. For a task, that means a terminal `FAILED` with
  `retryable=false` (or a dead-letter decision), not silent acceptance. For a cancel, it
  means leaving the task running rather than applying an unknown cancellation shape.
- Breaking changes are coordinated: do not publish a new version until the consumer
  supports it.

## 8. Idempotency summary

| Step | Idempotency key | Effect |
| --- | --- | --- |
| A publishes a task | `messageId` | Same task id on every outbox retry |
| B stores an incoming task | `messageId` | Duplicate task insert is a no-op, then the delivery is acked |
| B publishes an event | `(messageId, version)` | Redelivery reuses the same pair |
| A applies an event | `(messageId, version)` | Duplicate or older versions are ignored; terminal states are final |
| A cancels a task | `messageId` | Duplicate cancels are no-ops |

B consumes a task by inserting it into its inbox/work table with the task id as the key,
committing, and then acknowledging the AMQP delivery. A consumes events by applying them
in version order and persisting the task state in the same transaction.

## 9. Validating the examples

From the repository root, one command validates all examples and contract fixtures
against the draft 2020-12 schemas using `ajv-cli`:

```bash
npx --yes ajv-cli@5 validate --spec=draft2020 \
  -s documentation/schemas/task.schema.json \
  -r documentation/schemas/common.schema.json \
  -r documentation/schemas/payloads.schema.json \
  -d "documentation/schemas/examples/task.*.json" \
  -d "src/test/resources/contract/task.*.json" \
&& npx --yes ajv-cli@5 validate --spec=draft2020 \
  -s documentation/schemas/cancel.schema.json \
  -r documentation/schemas/common.schema.json \
  -d "documentation/schemas/examples/cancel.*.json" \
&& npx --yes ajv-cli@5 validate --spec=draft2020 \
  -s documentation/schemas/event.schema.json \
  -r documentation/schemas/common.schema.json \
  -r documentation/schemas/payloads.schema.json \
  -d "documentation/schemas/examples/event.*.json" \
  -d "src/test/resources/contract/event.completed.*.json"
```

## 10. Contract fixtures

The executable fixtures used by the Java drift test and by the future TypeScript contract
tests live in [`src/test/resources/contract/`](../src/test/resources/contract/):

| Fixture | Covers |
| --- | --- |
| `task.<task type>.json` | One task message for every registered `@Task` type |
| `event.completed.<task type>.json` | One `COMPLETED` event for every registered `@Result` type |

Each fixture is identical to the matching file in `schemas/examples/`. The validation
command in section 9 checks both sets. `ContractDriftTest` fails when a registered task or
result has no fixture, when a fixture does not decode through `TaskCodec`, or when
re-encoding changes the task payload.

## 11. File index

| File | Purpose |
| --- | --- |
| `schemas/common.schema.json` | UUID, instant, nickname, money, `WorldPoint`, `ItemAmount`, `InventoryDiscrepancy` |
| `schemas/payloads.schema.json` | Task payloads, result payloads, generic event payloads |
| `schemas/task.schema.json` | Task message envelope and per-type payload dispatch |
| `schemas/cancel.schema.json` | Cancel message envelope |
| `schemas/event.schema.json` | Event envelope and per-type payload dispatch |
| `schemas/examples/` | One example per task type, one per result type, plus cancel and non-completed events |
| `src/test/resources/contract/` | Executable task and completed-result fixtures shared by Java and TypeScript tests |
