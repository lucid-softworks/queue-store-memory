import {
  QueueJobNotFoundError,
  type QueueJob,
} from "@lucid-softworks/queue-core";
import { describe, expect, it } from "vitest";

import { MemoryQueueStore } from "../src/index.js";

const job = (id: string, overrides: Partial<QueueJob> = {}): QueueJob => ({
  attempt: 0,
  availableAt: 0,
  createdAt: 0,
  data: id,
  id,
  maxAttempts: 2,
  name: "work",
  priority: 0,
  state: "waiting",
  updatedAt: 0,
  ...overrides,
});

describe("MemoryQueueStore", () => {
  it("stores, replaces, deletes, and clears jobs", () => {
    const store = new MemoryQueueStore();
    expect(store.add(job("a"))).toBe(true);
    expect(store.add(job("a"))).toBe(false);
    expect(store.size).toBe(1);
    expect(store.get("a")?.data).toBe("a");
    store.save(job("a", { data: "changed" }));
    expect(store.list()[0]?.data).toBe("changed");
    expect(() => store.save(job("missing"))).toThrow(QueueJobNotFoundError);
    expect(store.delete("a")).toBe(true);
    expect(store.delete("a")).toBe(false);
    store.add(job("b"));
    store.clear();
    expect(store.size).toBe(0);
  });

  it("claims eligible jobs deterministically and renews matching leases", () => {
    const store = new MemoryQueueStore();
    store.add(job("future", { availableAt: 20, state: "scheduled" }));
    store.add(job("other", { name: "other", priority: 10 }));
    store.add(job("old", { createdAt: -1, priority: 1 }));
    store.add(job("new", { createdAt: 1, priority: 1 }));
    expect(() =>
      store.claim({ leaseDuration: 10, now: 0, workerId: "" }),
    ).toThrow(TypeError);
    expect(() =>
      store.claim({ leaseDuration: 0, now: 0, workerId: "worker" }),
    ).toThrow(RangeError);
    const claimed = store.claim({
      leaseDuration: 10,
      names: ["work"],
      now: 0,
      workerId: "worker",
    });
    expect(claimed?.id).toBe("old");
    expect(claimed?.attempt).toBe(1);
    expect(claimed?.lease?.token).toBe("worker-1");
    expect(() => store.heartbeat("old", "worker-1", 1, 0)).toThrow(RangeError);
    expect(store.heartbeat("missing", "x", 1, 10)).toBeUndefined();
    expect(store.heartbeat("old", "wrong", 1, 10)).toBeUndefined();
    expect(store.heartbeat("old", "worker-1", 1, 10)?.lease?.expiresAt).toBe(
      11,
    );
    expect(
      store.claim({ leaseDuration: 10, now: -1, workerId: "none" }),
    ).toBeUndefined();
  });

  it("recovers expired leases and filters active deduplication keys", () => {
    const store = new MemoryQueueStore();
    store.add(
      job("expired", {
        attempt: 1,
        lease: { expiresAt: 5, token: "old", workerId: "old" },
        priority: 1,
        state: "active",
      }),
    );
    store.add(
      job("live", {
        lease: { expiresAt: 20, token: "live", workerId: "old" },
        state: "active",
      }),
    );
    store.add(job("dedupe", { deduplicationKey: "key" }));
    store.add(
      job("done", {
        deduplicationKey: "finished",
        state: "completed",
      }),
    );
    expect(store.findByDeduplicationKey("key")?.id).toBe("dedupe");
    expect(store.findByDeduplicationKey("finished")).toBeUndefined();
    expect(store.findByDeduplicationKey("missing")).toBeUndefined();
    const reclaimed = store.claim({
      leaseDuration: 10,
      names: ["work"],
      now: 5,
      workerId: "new",
    });
    expect(reclaimed?.id).toBe("expired");
    expect(reclaimed?.attempt).toBe(2);
    expect(store.heartbeat("done", "x", 5, 10)).toBeUndefined();
  });
});
