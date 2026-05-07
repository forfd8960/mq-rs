# mq-rs

A lightweight in-memory message queue written in Rust, with:

- TCP message broker (custom length-delimited protocol)
- HTTP management API (create/list/query topics)
- Producer and consumer client modules

This README reflects the current code in `src/` and `examples/`.

## Features

- In-memory topic registry
- Publish/subscribe over TCP
- Topic and channel metadata query over HTTP
- Async I/O with Tokio

## Architecture

At runtime the binary starts two services in parallel:

1. TCP queue server (`0.0.0.0:5050`) for `PUB`/`SUB`
2. HTTP API server (`0.0.0.0:6060`) for topic management

`main` creates a shared `Arc<RwLock<MQ>>` and spawns:

- `start_queue(...)` in `src/mq.rs`
- `start_http(...)` in `src/http_server.rs`

### Data flow

1. Producer connects to TCP, writes magic header `RQV0`, then sends framed `PUB <topic>\n<body>`.
2. Server parses event, converts body into `Message`, and publishes into the topic broadcast channel.
3. Consumer connects to TCP, writes `RQV0`, sends framed `SUB <topic> <channel>\n`.
4. Server subscribes consumer to the topic stream and pushes framed message responses.

## Core Data Structures

### `MQ` (`src/mq.rs`)

Top-level broker state:

- `opts: Options` server/runtime config
- `topic_map: HashMap<String, Topic>` all topics
- `clients: HashMap<ClientID, Client>` connected clients
- `client_id_seq: u64` client id sequence

### `Topic` (`src/topic.rs`)

Represents a topic and routing primitives:

- `name`, `msg_count`, `msg_size`
- `memory_msg_chan_sender` / `memory_msg_chan_recv` (`mpsc` pair)
- `channel_msg_sender` (`broadcast::Sender<Message>`) for fanout
- `chan_map: Arc<RwLock<HashMap<String, Channel>>>`

### `Channel` (`src/channel.rs`)

Subscription grouping metadata:

- `name`, `topic`
- `memory_msg_chan: broadcast::Receiver<Message>`
- `clients: HashMap<ClientID, mpsc::Sender<Message>>`

### `Message` (`src/message.rs`)

Queue payload object:

- `id: Uuid`
- `body: Vec<u8>`
- `ts: u128` (nanoseconds since UNIX epoch)
- `attempts: u16`

`encode_msg` serializes as:

- 16 bytes timestamp
- 2 bytes attempts
- 16 bytes UUID
- remaining bytes payload body

## Wire Protocol (TCP)

### Handshake

- Client must send first 4 bytes: `RQV0`

### Framing

- Uses `tokio_util::codec::LengthDelimitedCodec`
- Frame payload layout after length prefix:
	- 4-byte frame type (`u32`)
	- frame data bytes

Frame type values (`src/protocol.rs`):

- `0`: Response
- `1`: Error
- `2`: Message

### Events

- `PUB <topic>\n<raw-bytes>`
- `SUB <topic> <channel>\n`

## HTTP API

Server: `http://127.0.0.1:6060`

- `POST /api/v1/topics`
	- body: `{ "topic": "orders" }`
- `GET /api/v1/topics`
- `GET /api/v1/topics/{topic_name}`

Examples are also in `http_server.rest`.

## Run

Start broker:

```bash
cargo run
```

Create a topic (required before publish/subscribe):

```bash
curl -X POST http://127.0.0.1:6060/api/v1/topics \
	-H 'content-type: application/json' \
	-d '{"topic":"orders"}'
```

## Usage Example: Consumer

Terminal A:

```bash
cargo run --example consumer -- orders analysis
```

Or use API directly in code:

```rust
use mq_rs::consumer::consume;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
		let stream = TcpStream::connect("0.0.0.0:5050").await?;
		let mut rx = consume(stream, "orders", "analysis").await?;

		while let Some(msg) = rx.recv().await {
				println!("received: {}", String::from_utf8_lossy(&msg.body));
		}
		Ok(())
}
```

## Usage Example: Producer

Terminal B:

```bash
cargo run --example producer -- orders "hello from producer"
```

Or use API directly in code:

```rust
use mq_rs::{producer::Producer, protocol::build_r_w_codec};
use tokio::{io, net::TcpStream};
use tokio_util::codec::FramedWrite;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
		let mut stream = TcpStream::connect("0.0.0.0:5050").await?;

		let mut producer = Producer::new("0.0.0.0:5050");
		producer.init_client(&mut stream).await?;

		let (_read_half, write_half) = io::split(stream);
		let (_read_codec, write_codec) = build_r_w_codec();
		let mut writer = FramedWrite::new(write_half, write_codec);

		producer
				.pub_msg(&mut writer, "orders", b"hello from producer".to_vec())
				.await?;

		Ok(())
}
```

## Notes on Current Behavior

- Topics must be created first through the HTTP API; `PUB` to unknown topics returns an error.
- Current consumer message pump subscribes by topic stream; channel name is tracked in metadata and HTTP output.
- `msg_count` and `msg_size` fields exist but are not yet incremented in publish flow.

## Project Layout

```text
src/
	main.rs         # starts TCP + HTTP servers
	mq.rs           # MQ state + TCP accept loop
	client.rs       # per-connection protocol handler
	protocol.rs     # event/frame encode/decode
	message.rs      # Message model + binary encoding
	topic.rs        # Topic and channel registry
	channel.rs      # Channel model
	http_server.rs  # REST API for topic management
	producer/       # producer helper API
	consumer/       # consumer helper API
examples/
	producer.rs
	consumer.rs
	read_write.rs
```
