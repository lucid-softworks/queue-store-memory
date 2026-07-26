# `@lucid-softworks/queue-store-memory`

An atomic, deterministic in-memory `QueueStore` for applications and tests.

```ts
import { MemoryQueueStore } from "@lucid-softworks/queue-store-memory";

const store = new MemoryQueueStore();
```

Claims select eligible jobs by descending priority, creation time, then id.
Expired active leases are returned to `waiting` before each claim. Heartbeats
only renew the current lease token. Active deduplication keys ignore terminal
jobs. Data is process-local and intentionally not durable across restarts.
