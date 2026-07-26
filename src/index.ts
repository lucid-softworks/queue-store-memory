import {
  isQueueJobTerminal,
  QueueJobNotFoundError,
  type QueueClaimOptions,
  type QueueJob,
  type QueueStore,
} from "@lucid-softworks/queue-core";
import { compareQueuePriority } from "@lucid-softworks/queue-priority";

function withoutLease(
  job: QueueJob,
): Omit<QueueJob, "lease"> & { lease?: never } {
  const { lease: _lease, ...released } = job;
  return released;
}

/** In-memory store with atomic synchronous claims and expiring leases. */
export class MemoryQueueStore implements QueueStore {
  readonly #jobs = new Map<string, QueueJob>();
  #leaseSequence = 0;

  get size(): number {
    return this.#jobs.size;
  }

  add(job: QueueJob): boolean {
    if (this.#jobs.has(job.id)) return false;
    this.#jobs.set(job.id, job);
    return true;
  }

  get(id: string): QueueJob | undefined {
    return this.#jobs.get(id);
  }

  list(): readonly QueueJob[] {
    return [...this.#jobs.values()];
  }

  claim(options: QueueClaimOptions): QueueJob | undefined {
    if (options.workerId.length === 0)
      throw new TypeError("workerId cannot be empty");
    if (!Number.isFinite(options.leaseDuration) || options.leaseDuration <= 0)
      throw new RangeError("leaseDuration must be positive and finite");

    for (const job of this.#jobs.values()) {
      if (
        job.state === "active" &&
        job.lease !== undefined &&
        job.lease.expiresAt <= options.now
      ) {
        this.#jobs.set(job.id, {
          ...withoutLease(job),
          availableAt: options.now,
          state: "waiting",
          updatedAt: options.now,
        });
      }
    }

    const candidates = [...this.#jobs.values()].filter(
      (job) =>
        (job.state === "waiting" || job.state === "scheduled") &&
        job.availableAt <= options.now &&
        (options.names === undefined || options.names.includes(job.name)),
    );
    // Claim order is stable and the store collection itself is not mutated.
    candidates.sort(compareQueuePriority);
    const candidate = candidates[0];
    if (candidate === undefined) return undefined;
    const claimed: QueueJob = {
      ...candidate,
      attempt: candidate.attempt + 1,
      lease: {
        expiresAt: options.now + options.leaseDuration,
        token: `${options.workerId}-${String(++this.#leaseSequence)}`,
        workerId: options.workerId,
      },
      state: "active",
      updatedAt: options.now,
    };
    this.#jobs.set(claimed.id, claimed);
    return claimed;
  }

  save(job: QueueJob): void {
    if (!this.#jobs.has(job.id)) throw new QueueJobNotFoundError(job.id);
    this.#jobs.set(job.id, job);
  }

  heartbeat(
    id: string,
    leaseToken: string,
    now: number,
    leaseDuration: number,
  ): QueueJob | undefined {
    if (!Number.isFinite(leaseDuration) || leaseDuration <= 0)
      throw new RangeError("leaseDuration must be positive and finite");
    const job = this.#jobs.get(id);
    if (
      job?.state !== "active" ||
      job.lease === undefined ||
      job.lease.token !== leaseToken
    )
      return undefined;
    const renewed: QueueJob = {
      ...job,
      lease: { ...job.lease, expiresAt: now + leaseDuration },
      updatedAt: now,
    };
    this.#jobs.set(id, renewed);
    return renewed;
  }

  delete(id: string): boolean {
    return this.#jobs.delete(id);
  }

  findByDeduplicationKey(key: string): QueueJob | undefined {
    return [...this.#jobs.values()].find(
      (job) => job.deduplicationKey === key && !isQueueJobTerminal(job),
    );
  }

  clear(): void {
    this.#jobs.clear();
  }
}
