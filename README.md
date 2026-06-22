# JoSch (Job Scheduler)

JoSch is a memory-bound, resilient background job scheduling engine written from scratch in Go. It operates without an external database, utilizing an in-memory storage layer backed by a custom Write-Ahead Log (WAL) to guarantee durability and crash recovery.

## Core Engineering Decisions & Architecture

### 1. Dual-Lane Ingestion & Execution Pipeline
To protect system throughput, incoming jobs are routed immediately based on their execution timestamp:
* **Immediate Lane ($O(1)$ Pass-Through):** Jobs requiring instant execution bypass the priority heap entirely and are dropped straight into an un-sorted channel (`immediateCh`). This prevents heavy heap-rebalancing taxes under intense immediate traffic.
* **Delayed Lane ($O(\log N)$ Min-Heap):** Time-sensitive tasks are pushed to a thread-safe min-heap, maintaining strict execution order based on priority/time.
* **Unified Worker Pool:** Both lanes funnel into a single, bounded worker channel (`JobChannel`). This guarantees absolute control over resource consumption (CPU/DB connections) and enforces a predictable memory ceiling.

### 2. State-Assessment Multiplexer (The Dispatcher Loop)
The background orchestrator uses a non-polling, resource-efficient state machine:
* It locks and peeks at the top element of the min-heap to compute the sleep duration.
* It drops its lock and utilizes a persistent `time.Timer` within a multiplexing `select` block to sleep efficiently.
* The loop stays alert to native Go channel communication: incoming immediate jobs or higher-priority ready signals instantly interrupt the timer sleep, forcing a dynamic state re-evaluation without wasting CPU cycles.

### 3. Durability Layer: State-Tracking WAL & Map Recovery
Since the engine operates in-memory, state durability is achieved via an append-only **JSON Lines (JSONL)** Write-Ahead Log:
* **Atomic State Transitions:** System modifications flush sequentially to disk (`os.O_APPEND`) at three core boundaries: `SCHEDULED`, `IN_PROCESS`, and `COMPLETED`/`FAILED`.
* **Blueprint Reconstruction on Boot:** On startup, the system parses the WAL file linearly from line 1 to EOF, aggregating records into an isolated tracking map. `SCHEDULED` adds a job; `COMPLETED`/`FAILED` deletes it. Active queues are populated only *after* the file stream ends, completely removing the need for a live database LSN pointer and preventing race conditions during recovery.
* **Atomic Log Compaction:** To mitigate unbounded file growth, a compaction routine takes a snapshot of currently alive in-memory tasks, marshals them into a temporary file (`app.wal.tmp`), and uses an atomic `os.Rename()` operation to replace the bloated historical log safely.
