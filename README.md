# Synapse

A high-performance, lightweight, tiered-storage streaming commit log engine implemented from scratch in **pure Go** (zero external dependencies).

Synapse combines the append-only commit-log model of modern distributed streaming systems with a cloud-native **tiered storage architecture**: ultra-low-latency hot writes and reads on local NVMe storage, paired with asynchronous, background offloading of sealed segments to durable, cost-effective object storage (S3 / MinIO).

---

## Why Synapse Exists: The Problems with Apache Kafka

Apache Kafka is the de facto standard for distributed event streaming, but it was designed in an era of static bare-metal servers and spinning disks. For modern cloud-native architectures, Kafka introduces severe operational, financial, and architectural liabilities:

### 1. Tightly-Coupled Compute and Storage
In traditional Kafka, storage is bound directly to broker nodes (via local NVMe or AWS EBS volumes). 
- **Inflexible Scaling**: When storage needs grow, you must add more broker nodes—paying for unneeded CPU and memory just to get disk capacity. Conversely, if compute needs spike, you provision extra nodes with redundant disk capacity.
- **Partition Rebalancing Hell**: Adding, removing, or replacing brokers triggers partition reassignment. Kafka must copy gigabytes or terabytes of partition data across the network between brokers. This saturates broker network links and disk I/O, often causing cascading failures when the cluster is already under distress.

### 2. Operational Complexity and JVM Bloat
- **Resource Heavy**: Kafka runs on the JVM and requires substantial memory footprints, complex heap sizing, garbage collection tuning (to avoid stop-the-world GC pauses that trigger broker heartbeating timeouts), and deep OS page-cache management.
- **Cluster Choreography**: Whether managing ZooKeeper or Kafka Raft (KRaft) metadata quorums, operating a healthy Kafka cluster requires dedicated site reliability and data platform engineering teams.
- **High Operational Barrier**: For small-to-midsize engineering teams or internal services, deploying and maintaining a Kafka cluster is massive operational overkill.

### 3. Exorbitant Cost of Long-Term Retention
- Storing weeks, months, or infinite event logs on high-performance local NVMe or provisioned EBS volumes costs **$0.08–$0.125 per GB-month** (often replicated 3x across availability zones, pushing effective costs to **$0.24–$0.375 per GB-month**).
- Cloud object storage (AWS S3, Google Cloud Storage, Cloudflare R2, MinIO) costs **$0.015–$0.023 per GB-month** (or significantly less for colder tiers) with 99.999999999% (11 9's) built-in durability.
- Kafka’s late-arrival tiered storage implementations remain complex, require enterprise licensing or specialized plugins, and complicate partition maintenance.

### 4. Wire Protocol & Ecosystem Accidental Complexity
- The Apache Kafka wire protocol is extraordinarily complex, comprising hundreds of API keys, versioned request/response schemas, record batch envelopes, compacted bitmasks, variable-length zig-zag encoding, transaction coordinators, group rebalances, and SASL handshakes.
- Many systems do not actually need Kafka's ecosystem baggage. They simply need a reliable, fast, append-only durable log with deterministic offsets and tiered offloading.

### 5. Durability vs. Latency Dilemma
- By default, Kafka relies on the OS page cache and replication across nodes without synchronously calling `fsync` on every write. If power fails across a rack or OS kernels crash simultaneously, unfsync'd data in the OS page cache is vulnerable.
- Conversely, calling `fsync` synchronously for every single write caps throughput at the physical IOPS limit of the drive. Modern streaming engines require bounded, intelligent group commits and explicit tiered storage to bridge this gap.

---

## Architectural Overview

Synapse was designed from the ground up to solve these problems through a clean, tiered-log design:

```
                  ┌───────────────────────────────┐
                  │          HTTP Client          │
                  └──────────────┬────────────────┘
                                 │
                         POST /produce
                          GET /fetch
                                 │
                                 ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                      Synapse Broker                         │
   │                                                             │
   │   ┌─────────────────────────────────────────────────────┐   │
   │   │                    Partition                        │   │
   │   │                                                     │   │
   │   │  [Active Segment]   [Sealed Seg 1]   [Sealed Seg 2] │   │
   │   │   (Resident NVMe)    (Flushed)        (Flushed)     │   │
   │   │        │                  │                │        │   │
   │   └────────┼──────────────────┼────────────────┼────────┘   │
   └────────────┼──────────────────┼────────────────┼────────────┘
                │                  │                │
        sync append (NVMe)         │ background     │ on-demand cold
                │                  │ flush loop     │ read & re-cache
                ▼                  ▼                ▼
       ┌─────────────────┐       ┌────────────────────────────┐
       │   Local NVMe    │       │     S3 / MinIO Storage     │
       │  Fast Storage   │       │   Durable Object Storage   │
       └─────────────────┘       └────────────────────────────┘
```

### Key Principles

1. **Pure Go, Zero Dependencies**: Synapse relies exclusively on the Go standard library. It includes a custom, zero-dependency AWS Signature Version 4 (SigV4) S3 client.
2. **Tiered Storage Lifecycle**:
   - **Hot Write Edge**: Writes append to an active segment file on local disk (NVMe). Each record is length-prefixed and CRC32-checksummed. Offsets are strictly monotonic and gapless (64-bit integers).
   - **Segment Rolling**: Once the active segment reaches `maxSegmentBytes`, it is sealed and a new active segment is created.
   - **Background Object Offload**: A periodic flush loop detects sealed segments, uploads them via pure-Go S3 client to object storage (`partition-N/00000000000000000000.log`), and deletes the local files to immediately reclaim NVMe disk space.
   - **Smart Reads & Re-Caching**:
     - *Hot Reads* (tailing consumers reading the active or local segment) are served directly from NVMe with zero network latency.
     - *Cold Reads* (historical backfills reading flushed segments) automatically download the segment from S3, write it to local disk, rebuild the frame index in memory, and serve the record.
     - Subsequent reads against that segment hit the local cache, paying the network roundtrip **exactly once per segment, not once per record**.
3. **Robust Crash Recovery**: Each segment frame contains a length header and IEEE CRC32 checksum. Upon startup, Synapse scans frames, validates checksums, and safely truncates corrupt or partially written trailing frames.

---

## Project Structure

```
.
├── cmd/
│   ├── broker/               # Standalone Synapse broker daemon
│   │   └── main.go
│   └── demo/                 # End-to-end interactive demonstration
│       └── main.go
├── internal/
│   ├── broker/               # HTTP REST API routing and lifecycle server
│   │   ├── server.go         # Produce, fetch, health check, background flush loop
│   │   └── server_test.go
│   ├── config/               # Environment-based configuration loader
│   │   └── config.go
│   ├── objectstore/          # Pluggable storage abstraction
│   │   ├── store.go          # ObjectStore interface (Put, Get, Delete)
│   │   ├── s3_client.go      # Zero-dependency AWS SigV4 S3/MinIO client
│   │   ├── s3_client_test.go
│   │   ├── memory_store.go   # In-memory store for unit tests
│   │   └── errors.go
│   └── storage/              # Core commit log engine
│       ├── partition.go      # Multi-segment partition manager, roll & flush logic
│       ├── partition_test.go
│       ├── segment.go        # Low-level segment file I/O, binary indexing, CRC checks
│       ├── record.go         # Framing encoding/decoding: [4-byte len][4-byte CRC][payload]
│       └── errors.go
└── go.mod
```

---

## Getting Started

### Prerequisites
- **Go**: Version 1.22+ or 1.25+
- Optional: MinIO or AWS S3 credentials (for tiered offloading in production)

### 1. Run the Interactive Demo
The demo verifies the end-to-end lifecycle: local write, crash-durability sync, segment rolling, background S3 flush, disk space reclamation, hot NVMe reads, and cold S3 downloads with local re-caching:

```bash
go run cmd/demo/main.go
```

**Output Walkthrough**:
```text
[1] local data directory (stand-in for NVMe): /tmp/streamdb-demo-xxxxxx
[2] object store ready (in-memory fake standing in for MinIO/S3)
[3] partition 0 opened (max segment size: 200 bytes, tiny on purpose)
[4] appending 60 records...
    done. next offset to be assigned: 60
    segments created by rolling: 4 (only the last one is still active/writable)
[5] fsync'd the active segment - all 60 records are now crash-durable on local disk
[6] before flush: 4 of 4 segments are resident on local disk
    object store Put calls so far: 0
[7] flushed 3 sealed segment(s) to object storage
    object store Put calls now: 3
    after flush: only 1 of 4 segments remain resident on local disk (the active one)
[8] HOT read at offset 59 -> "event-059"
    object store Get calls: 0 -> 0 (served entirely from local disk)
[9] COLD read at offset 0 -> "event-000"
    object store Get calls: 0 -> 1 (+1: downloaded the segment)
    that segment is now re-cached locally: 2 of 4 segments resident on disk
[10] repeat read at offset 0 -> "event-000"
     object store Get calls: 1 -> 1 (served from local re-download cache)
```

### 2. Run the Unit Tests
Run all unit and integration tests across the storage engine, HTTP broker, and S3 client:

```bash
go test -v ./...
```

### 3. Run the Broker Server
Launch the HTTP broker daemon:

```bash
go run cmd/broker/main.go
```

By default, the server starts on `http://localhost:8080`.

---

## Configuration Reference

Synapse is configured entirely via environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `BROKER_ID` | `broker-1` | Unique broker identifier |
| `DATA_DIR` | `./data` | Local directory for partition and segment data |
| `MAX_SEGMENT_BYTES` | `67108864` (64 MB) | Maximum size of an active segment before rolling |
| `HTTP_LISTEN_ADDR` | `:8080` | Host and port for the HTTP produce/fetch API |
| `FLUSH_INTERVAL` | `5s` | Interval for background offloading of sealed segments |
| `S3_ENDPOINT` | `localhost:9000` | S3 / MinIO host endpoint |
| `S3_BUCKET` | `streamdb` | Destination S3 bucket name |
| `S3_ACCESS_KEY` | `minioadmin` | AWS / MinIO Access Key |
| `S3_SECRET_KEY` | `minioadmin` | AWS / MinIO Secret Key |
| `S3_USE_SSL` | `false` | Enable HTTPS for S3 communication |

---

## HTTP API Usage

### Produce a Record
```bash
curl -X POST "http://localhost:8080/produce?topic=events&partition=0" \
     -H "Content-Type: application/octet-stream" \
     -d '{"user_id": "usr_42", "event": "checkout_completed", "amount": 99.50}'
```
**Response** (`200 OK`):
```json
{
  "topic": "events",
  "partition": 0,
  "offset": 0
}
```

### Fetch a Record by Offset
```bash
curl "http://localhost:8080/fetch?topic=events&partition=0&offset=0"
```
**Response** (`200 OK`):
```json
{"user_id": "usr_42", "event": "checkout_completed", "amount": 99.50}
```

### Health Check
```bash
curl "http://localhost:8080/healthz"
```
**Response** (`200 OK`):
```text
ok
```

---

## Future Roadmap & Architecture Improvements

The current version of Synapse validates the tiered-storage streaming engine. The roadmap outlines prioritized enhancements to scale throughput, resilience, and integration:

### 1. Batch Produce Endpoint (Multi-Record Ingestion)
- **Problem**: The current `POST /produce` accepts a single record per HTTP request. At high ingestion rates, HTTP connection framing, JSON decoding, and network roundtrips become the primary bottleneck.
- **Planned Solution**: Introduce a `POST /produce/batch` endpoint that accepts multiple records in a single payload (e.g. streaming binary format or framed chunks). Records will be appended in a single locked critical section, dramatically cutting lock contention and network overhead.

### 2. Batch fsync / Group Commit
- **Problem**: Calling `p.Sync()` synchronously after every individual append forces a physical drive write per record, capping single-partition throughput to the disk's IOPS limit (~10,000–80,000 writes/sec on NVMe).
- **Planned Solution**: Implement a **Group Commit** mechanism:
  - Acknowledge writes after either $N$ milliseconds (e.g., 2–5 ms latency window) or $N$ accumulated requests/bytes.
  - Incoming appends join an active batch queue; a single worker thread executes `fsync` once for the entire batch and unblocks all waiting requests simultaneously.

### 3. Persisted Segment Index & Partition Manifest
- **Problem**: When sealed segments are uploaded to S3, the local files are removed to reclaim disk space. Currently, `OpenPartition` discovers segments by scanning the local directory (`os.ReadDir`). If the broker restarts, it only sees local segments and "forgets" earlier segments that were offloaded to S3.
- **Planned Solution**: Introduce a persistent partition manifest (e.g., `manifest.json` or an append-only metadata log `manifest.meta` per partition):
  - Tracks all segments across both tiers: start offset, record count, byte size, status (`ACTIVE`, `SEALED_LOCAL`, `REMOTE_S3`), and remote S3 key.
  - On startup, the broker reads the manifest and immediately restores the full offset space from $0$ to $N$, ensuring seamless cold reads across broker restarts.

### 4. Pragmatic Wire Protocol Strategy
- **Why Not Start with Kafka Wire Protocol?**
  Supporting Kafka's full binary wire protocol upfront introduces enormous incidental complexity (hundreds of RPC messages, complex version schemas, consumer group coordinators, transactional state machines) before the core storage engine is bulletproof.
- **The Pragmatic Path**:
  - Many production systems (such as internal event logs, telemetry pipelines, and database replication engines) ship successfully with lightweight custom binary protocols (e.g., gRPC, Cap'n Proto, or simple framed TCP streams).
  - Synapse will stabilize its core storage engine, batching, and manifest index first.
  - **Only then** will we consider implementing a subset of the real Kafka wire protocol (`ProduceRequest` v7+, `FetchRequest` v11+, `MetadataRequest`), and only if direct compatibility with existing Kafka ecosystem clients (librdkafka, sarama, kafka-python) is strictly required for the target use case.
