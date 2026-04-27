# Message Broker V0 (Rust) - MVP Blueprint

## 1. V0 Goal

Build a small, reliable, single-node message broker that supports:

1. Publish messages to a topic.
2. Consume messages from a topic channel.
3. At-least-once delivery (ack and retry).
4. Basic persistence (survive process restart).

Do not optimize for clustering, discovery, or advanced routing yet.

## 2. What MVP Looks Like

### 2.1 Must-have features

1. Single binary server, single process, single machine.
2. Topic and channel model:
   1. Topic receives published messages.
   2. Channel is a subscription queue under a topic.
3. Producer API:
   1. publish(topic, payload)
4. Consumer API:
   1. subscribe(topic, channel)
   2. receive()
   3. ack(message_id)
   4. nack(message_id, delay_ms)
5. Message timeout and redelivery.
6. Memory-first queue with disk fallback (or always-disk if you want simpler first cut).
7. Graceful restart with metadata recovery.

### 2.2 Explicitly out of scope for V0

1. Multi-node clustering.
2. Service discovery.
3. Exactly-once semantics.
4. Multi-tenant auth/ACL.
5. Compression, TLS, protocol negotiation.
6. Complex ordering guarantees across channels.
7. Rich UI and admin dashboard.

## 3. Core Data Structures to Implement First

Implement in this exact order.

### 3.1 Message

Purpose: immutable unit of data.

Suggested fields:

1. id: u128 (or [u8; 16])
2. topic: String (optional if context already has it)
3. body: bytes::Bytes or Vec<u8>
4. timestamp_ns: i64
5. attempts: u16
6. defer_until: Option<Instant or epoch_ms>

Why first:

- Every path (publish, persist, dispatch, retry, ack) depends on this type.

### 3.2 Broker registry

Purpose: global in-memory index.

Suggested fields:

1. topics: HashMap<TopicName, Arc<Topic>>
2. shutdown signal and task handles
3. config

Why second:

- Lets you resolve publish and subscribe paths through one state root.

### 3.3 Topic

Purpose: fan-out coordinator.

Suggested fields:

1. name
2. channels: HashMap<ChannelName, Arc<Channel>>
3. ingress queue for published messages (tokio::mpsc)
4. optional disk log handle
5. topic message pump task handle

Why third:

- Topic pump is the center of fan-out behavior.

### 3.4 Channel

Purpose: per-subscriber-group delivery state machine.

Suggested fields:

1. name
2. ready queue (tokio::mpsc or VecDeque + Notify)
3. in_flight: HashMap<MessageId, InFlightEntry>
4. deferred: BinaryHeap or BTreeMap keyed by due time
5. subscribers: HashMap<ClientId, Sender<Delivery>>
6. counters: message_count, timeout_count, requeue_count

Why fourth:

- At-least-once semantics live here.

### 3.5 Persistent queue abstraction

Purpose: decouple broker logic from storage.

Define trait first:

1. put(bytes) -> Result
2. read_next() -> Option<bytes>
3. ack_cursor() / commit read progress
4. depth() -> usize
5. close()

V0 implementation options:

1. Simpler: append-only log file + metadata cursor.
2. Better V0: segmented files (roll by size) + metadata file.

Why fifth:

- You can ship memory-only quickly, then add persistence behind same interface.

### 3.6 Client session

Purpose: track consumer state.

Suggested fields:

1. client_id
2. subscribed topic/channel
3. ready_count (prefetch credit)
4. in_flight_count
5. msg_timeout

Why sixth:

- Required for flow control and timeout correctness.

## 4. Main V0 Data Flow

Design this first, then code APIs around it.

### 4.1 Publish flow

1. Producer sends publish(topic, payload).
2. Broker resolves or creates topic.
3. Topic validates size and builds Message.
4. Message enters topic ingress queue.
5. Topic pump fans out to all channels:
   1. clone lightweight metadata/body handle
   2. push to channel ready queue (or channel disk fallback)

V0 success criteria:

- publish returns success after message is accepted by topic ingress (or durable append if you want stronger guarantee).

### 4.2 Subscribe flow

1. Consumer sends subscribe(topic, channel).
2. Broker resolves or creates topic and channel.
3. Channel registers client session.
4. Client starts receive loop.

V0 success criteria:

- subscribe returns only after registration is complete.

### 4.3 Delivery flow

1. Consumer sets readiness (implicit or explicit).
2. Channel dispatcher selects next available message.
3. Channel sends message to consumer.
4. Channel inserts message into in_flight with deadline.

V0 success criteria:

- channel never loses ownership of a sent-but-unacked message.

### 4.4 Ack/Nack flow

1. ack(message_id):
   1. remove from in_flight
   2. update counters
2. nack(message_id, delay):
   1. remove from in_flight
   2. increment attempts
   3. schedule into deferred queue

V0 success criteria:

- duplicate delivery is possible; silent loss is not acceptable.

### 4.5 Timeout and redelivery flow

1. Background timer scans in_flight deadlines.
2. Expired message:
   1. remove from in_flight
   2. increment timeout_count
   3. requeue to channel ready queue (or deferred queue)

V0 success criteria:

- stuck consumer cannot permanently block messages.

### 4.6 Restart recovery flow

1. On shutdown:
   1. flush write buffers
   2. persist metadata (read/write cursor, depth)
2. On startup:
   1. load metadata
   2. restore queue cursors
   3. replay pending messages into channel state

V0 success criteria:

- committed but undelivered messages are still available after restart.

## 5. Suggested V0 Architecture (Rust)

Use async runtime and task-per-component model:

1. Tokio runtime.
2. Topic task per topic (message fan-out loop).
3. Channel task per channel (delivery + timeout handling).
4. One storage task per persistent queue if needed.

Concurrency primitives:

1. Arc + tokio::sync::RwLock/Mutex for registries.
2. tokio::sync::mpsc for hot-path queues.
3. tokio::sync::Notify for wakeups.
4. tokio::time::interval for timeout scanner.

## 6. V0 Protocol Recommendation

Keep protocol very small first:

1. PUB topic size payload
2. SUB topic channel
3. ACK message_id
4. NACK message_id delay_ms
5. PING

Use line protocol + length-prefixed payload for simplicity.

## 7. V0 Milestones

### Milestone A: Memory-only broker

1. Topic/channel registry.
2. PUB/SUB/ACK/NACK.
3. In-flight and timeout retry.
4. Basic metrics logs.

### Milestone B: Persistence

1. Append-only segment writer.
2. Reader cursor + metadata file.
3. Restart recovery tests.

### Milestone C: Hardening

1. Backpressure limits.
2. Dead-letter option (optional).
3. Better error handling and graceful shutdown.

## 8. Minimal Test Matrix for V0

1. Publish then consume happy path.
2. Consumer disconnect before ack -> message redelivered.
3. Nack with delay -> delayed re-delivery.
4. Timeout with no ack -> requeue.
5. Restart broker -> pending messages still consumable.
6. Multiple channels under one topic each receive a copy.

## 9. Practical V0 Decision Rules

1. Prefer simple and explicit state transitions over optimization.
2. Keep a strict ownership model for message lifecycle:
   1. queued
   2. in-flight
   3. acked or requeued
3. Introduce disk abstraction early, even if first impl is basic.
4. Ship single-node reliability first, then scale-out design in V1.

## 10. One-line V0 Definition

V0 is successful when a single-node Rust broker can accept publishes, fan out to topic channels, deliver with ack/retry semantics, and recover pending messages after restart without silent loss.