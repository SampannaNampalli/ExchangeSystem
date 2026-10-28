# ExchangeServer2 — Vision (v6)

**Supersedes:** v5 and v4 (in git history; v5 extended the roadmap to multiple symbols, DevOps, distributed systems
and a public demo; v6 makes zero cost a constraint on every step) and `ExchangeServer_Vision_v3.md` (in the original
`Exchange Server` project). Self-contained.
**Companion:** `ExchangeServer2_Spec.md` (exact code state, decisions, open threads).
**Rule:** the roadmap lives only here. The spec points to it and does not duplicate it.

---

## 1. Purpose

ExchangeServer is a Java trade-exchange matching engine built as a **hands-on way to understand production-grade
software systems**. Depth of understanding matters more than feature count: every design choice should be
justified, tested, and understood well enough to defend.

**Honest scope note:** building something that *works like* a real exchange is achievable. Matching the scale
and robustness of NASDAQ/LMAX-class venues is not, solo. The aim is to hit the same design problems real systems
hit and understand why they were solved the way they were.

Learning order: correctness of the core -> concurrency model -> networking -> durability/recovery (all done) ->
delivery and observability -> multiple symbols -> operations and a public deployment -> distribution (replication,
then partitioning) -> the systems around an exchange.

The project already has three of the four parts of state machine replication: a deterministic engine, an ordered
command log, and CHECK digests that prove two replays agree. Distribution (§7 phases 5–6) adds the fourth, shipping
the log to other machines, rather than a redesign.

---

## 2. What the system is

Today, a single-instrument limit order book (multiple symbols are roadmap step 11) with a price-time-priority
matching engine, behind the full pipeline:

```
 clients --> TCP gateway --> one ordered queue --> writer thread --------------------------------> OrderBook
             (stateless       (backpressure:       stamp (seq, ts, id) -> journal (one fsync      (levels, nodes,
              validation)      BUSY when full)      per batch) -> engine (deterministic)            id map)
                                                            |                        |
                                                            v                        v
                                                    replay on restart        replies, passive fills,
                                                    (+ poison-pill skip)     market data (L2 + trades)
```

**Status (2026-10-27): roadmap steps 1–7 are implemented** in ExchangeServer2, a rewrite of ExchangeServer into a
Maven project with real packages. Steps 1–3 were rebuilt with the open threads of Spec v8 closed; steps 4–7 are
new. The rewrite was written by the LLM at the user's request (see §3). The same day, at the user's request, the
LLM also fixed every risk found by the study-guide review, including the deferred security findings: snapshots and
offline compaction, output sequence numbers and resend, self-trade prevention, authentication, TLS and per-session
limits (Spec D63–D77). Steps 8–31 (v5, same day) are planned and not started.

---

## 3. Working style (how to collaborate)

1. **Reason before code.** Walk a concrete numeric scenario in plain English before generalizing.
2. **Who writes what.** In ExchangeServer the user wrote the production code and the LLM reviewed, quizzed, and
   wrote the tests. **ExchangeServer2 was written entirely by the LLM on 2026-10-27, at the user's explicit
   request** ("I write it all", full roadmap). Going forward, the user decides per piece of work which mode applies;
   the default for new features remains the original loop below.
3. **Verify by tracing, not asserting.** Before calling anything "done", trace it with concrete inputs, including
   edge cases and exit paths.
4. **Call out weak justifications.** "Production systems do it" is not a reason unless true; the real reason is
   often simpler. Stale rationales must be fixed when the design changes.
5. **Check exhaustiveness explicitly.** Name the dimensions of a problem (e.g. 2 sides x 3 price relations = 6
   cases) rather than listing cases until it feels enough.
6. **The loop for user-written work:** the LLM gives a short description (no hints, no code) -> the user designs
   and codes -> the LLM reviews the saved code and shows where it breaks (location + concrete input, not the fix)
   -> the LLM tests the user's understanding -> the user fixes; repeat -> the LLM writes and runs the tests and
   reports mutation results.

*Instructions for an assisting LLM:* adopt the above.

---

## 4. Engineering principles

1. **Invariants first.** State the invariant, find the single place that maintains it, make violations fail loudly.
   - `totalVolume` equals the live remaining volume of the level's nodes at all times.
   - The id map contains an id iff that order rests in a level.
   - The book never contains empty price levels, and is never crossed after a command.
   - Invariants are protected at their source, not re-checked redundantly by callers.
2. **One owner per responsibility, enforced by the compiler.** `OrderBook` owns the order lifecycle; every
   `PriceLevel` mutator is package-private, so the engine (another package) can read levels but cannot change
   them. The engine decides what a match means; the book carries it out (`fillBest` returns an immutable `Fill`).
   *(Resolves the v3 `fillHeadOrder` tension.)*
3. **One code path where possible.** Fill-removal and cancel share `removeNode`/`unlink`; replay, the live path
   and the poison-pill rebuild all use the same engine.
4. **Determinism.** Engine behaviour depends only on the ordered input commands. Ids, timestamps and sequence
   numbers are stamped by the sequencer on the writer thread before journalling; no wall-clock, randomness or
   hash-order dependence in matching; one writer thread, enforced by construction.
5. **Inputs are the source of truth.** Log sequenced commands; derive outputs by replay.
6. **Validate at the right layer.** Stateless checks at the gateway (`OrderRequest`); state-dependent checks in the
   engine. Internal invariant violations throw; external bad requests return rejects.
7. **Measure before optimizing.** Done in step 6: the cached best-level pointer was measured and rejected; smart
   batching was measured and kept (Spec §5).
8. **Honest complexity claims.** Cancel / fill removal of a node O(1); removing an emptied level O(log L);
   best-level lookup O(log L) (measured: ~10–20 ns, under 2% of a command).
9. **Business logic and data structures stay separated.** Containers expose state; the engine decides its meaning.
10. **Fail-stop over limping.** A journal failure or an unexpected error halts the writer; the process exits;
    the journal is the truth on restart.

---

## 5. Key design commitments (detail in the Spec)

- Doubly linked list per price level with sentinel nodes and caller-held node handles; `orderId -> OrderNode` map;
  eager removal of emptied levels.
- Write-ahead input log, plain text, with a header naming the rules version; state-dependent rejects are logged,
  stateless rejects never reach the queue.
- **Concurrency:** one writer thread owns sequencer, journal, engine and book. Callers hand in requests through one
  queue and get a `CompletableFuture` each. No internal thread-safety in the book.
- **Batching:** smart batching (take whatever is waiting, one fsync, then process), chosen by measurement.
- **Poison pills:** quarantine + rebuild live; stop + operator quarantine at startup.
- **Recovery:** snapshots + offline compaction bound recovery; CHECK records prove replay rebuilt the same outputs.
- **Gateway:** TCP or TLS, line protocol, thread-per-connection, slow consumers disconnected; authenticated
  participants, loopback by default, per-session timeouts and limits; a numbered private stream per participant
  with RESEND; opt-in cancel-on-disconnect; self-trade prevention in the engine.

---

## 6. Non-goals (for now)

- Matching on several threads inside one process (parallel matching behind one journal). fsync dominates a
  command's cost (Spec §5), so scaling out is done by sharding (step 24) instead. Revisit only if a benchmark asks.
- Price-indexed array books (needs a bounded price range; benchmarks did not ask for it).
- Full FIX/ITCH compliance. A binary protocol is step 31; certification-grade compliance is not a goal.
- Real money. The system is a play-money simulation: no deposits, payouts or real securities, and the public demo
  uses made-up symbols, not real tickers or real market data.
- Paid infrastructure. Every step is built and run on free tooling and free tiers (§7, open decision 7): the full
  cluster runs locally (kind or k3d) and the public demo is one free-tier host. The one accepted cost is an optional
  domain name (about $10 a year).

Removed from this list in v5, now on the roadmap: multiple instruments, distributed replication and consensus,
accounts and permissions, online rotation, market-data resend. (Authentication and TLS left it on 2026-10-27, D74.)

---

## 7. Roadmap — single source of truth

Step numbers are stable: code comments cite them ("roadmap step 4A"). New steps get the next number, never a reused
one. Each phase leaves a working system; within a phase, steps are in dependency order.

### Built

| # | Step | State |
|---|---|---|
| 1 | Close open threads and O(1) cancel | done (v3), rebuilt in ES2 |
| 2 | Order types / cancel flows | done (v3), rebuilt in ES2 |
| 3 | Sequencer + event log | done (v3), rebuilt in ES2 with batch append, header, streaming replay |
| 4 | TCP gateway with a single-writer engine thread (4A thread model, 4B gateway, 4C batching) | **done (ES2)** |
| 5 | Risk checks + market data | **done (ES2)**: client order ids + duplicates, price band, market collar, open-order cap; L2 updates, trades, snapshots |
| 6 | Benchmarking | **done (ES2)**: JMH; cached pointer rejected; smart batching kept |
| 7 | Crash recovery via replay, including poison-pill policy | **done (ES2)** |
| — | Cleanup: one test file per production class with `@Nested` groups | **done (ES2)** |
| — | Risk fixes from the study-guide review: snapshots + compaction, output digest, journal checksums, private stream + RESEND, cancel-on-disconnect, self-trade prevention (cancel incoming), authentication + TLS + session limits, operability | **done (ES2, 2026-10-27, Spec D63–D77)** |

### Phase 1 — Delivery foundations (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 8 | Continuous integration | GitHub Actions: build and test on a Windows and Linux matrix, warning-free check, nightly PIT, JMH runs that fail on a latency regression; CodeQL, dependency and secret scanning. All free on a public repository (a private one has a monthly minute quota, Windows minutes counting double). Shared runners give noisy timings, so the JMH gate compares against a baseline run in the same job and fails only on a large slowdown | CI/CD, performance regression gates, DevSecOps |
| 9 | Containers | Multi-stage Dockerfile, minimal JRE from `jlink`, non-root user, volumes for the journal and users file; `docker compose` for the whole stack; amd64 and arm64 images (the free demo host is ARM, step 19); SBOM (Syft) and keyless-signed (cosign) images published to GHCR | Reproducible builds, image hygiene, supply-chain security |
| 10 | Metrics and structured logs | Queue depth, batch size, fsync latency histogram, rejects by reason, sessions; JSON logs; Prometheus and Grafana in the compose file | Observability basics; measuring what Spec §5 measured once, continuously |

### Phase 2 — Multiple symbols, one process (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 11 | Multiple symbols | Recommended design: one `MatchingEngine` per symbol, routed by the writer, behind the single sequencer, journal and writer thread (the engine and book stay single-instrument). Instrument definitions (tick size, band, caps) journalled and in snapshots, not only in config; journal format v4 and a `Rules.VERSION` bump; symbol in the protocol and market data, per-symbol subscriptions; cancel by order id via an id-to-symbol map; decide which limits are per symbol and which per participant; poison-pill rebuild of only the affected symbol | Routing above an unchanged core; reference data that replay depends on; format migration |
| 12 | Trading phases and admin commands | Opening and closing auctions, halts, circuit breakers per symbol; admin commands (add instrument, halt, resume) journalled like any other command | Market structure; operator actions as part of the deterministic log |

### Phase 3 — Gateway and operations (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 13 | Scalable gateway | NIO selectors, or virtual threads (needs Java 21), instead of two platform threads per connection; measure both | I/O models, measuring a concurrency change |
| 14 | Online compaction, backups, restore | Compaction and journal rotation while running; snapshots and journals copied to object storage (a local S3-compatible store such as Garage or SeaweedFS; off-site, a free tier such as Cloudflare R2 or Backblaze B2); a rehearsed restore | Operating stateful systems without downtime; disaster recovery |
| 15 | Accounts and permissions | Users managed while running (no restart), roles (trader, viewer, admin), limits per participant across sessions | Authorization, multi-tenant fairness |

### Phase 4 — Public demo, play money (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 16 | Web gateway | WebSocket (and HTTP) gateway in front of the same `OrderRequest` validation as the TCP gateway; the JDK has no WebSocket server, so hand-rolled RFC 6455 framing or a library (decision below) | Protocol adapters; one validation layer for several front doors |
| 17 | Web front end | Order entry, live L2 book, trade tape, own orders and fills; a read-only public Grafana | Real-time UIs over a streaming API |
| 18 | Bots | A market maker quoting both sides and random traders, as ordinary authenticated participants | Load that looks like a market; a self-test that never stops |
| 19 | Hosting | Docker Compose on one Oracle Cloud Always Free ARM VM (a real disk for the journal's fsync; fallback: Google Cloud's e2-micro with a small heap, or a home PC); Cloudflare Tunnel in front for TLS and DDoS protection with no inbound ports (or Caddy with Let's Encrypt on a free DuckDNS subdomain); the web front end on GitHub Pages or Cloudflare Pages; firewall, per-IP limits; scheduled backups; demo accounts; a visible "simulation, play money" notice. Not hosts that sleep when idle or have no persistent disk (Render's free tier), nor time-limited credits. Moves to primary-backup after step 20 | Deploying and running a public service; abuse protection |

### Phase 5 — Replication, one group (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 20 | Primary-backup replication | The primary streams journal batches to backups, which replay and compare CHECK digests; a reply is released only after a majority has fsynced the batch; an epoch (term) in the journal header and every record fences a former primary; manual failover | State machine replication, quorum durability, split brain and fencing |
| 21 | Failure testing | Fault injection (kill the primary, cut the network with Toxiproxy, full disk, slow fsync) and a history checker: every acknowledged order survives failover | Jepsen-style correctness testing; proving rather than hoping |
| 22 | Automatic leader election | Own Raft implementation, or a lease from etcd (decision below) | Consensus, leases, election timeouts |
| 23 | Rolling upgrades | Replicas must run identical logic or replay diverges: upgrade at a snapshot boundary, or gate new behaviour on a journalled version | Upgrading deterministic replicated systems; the generalization of `Rules.VERSION` |

### Phase 6 — Partitioning and orchestration (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 24 | Sharding | N replicated groups, each owning a set of symbols; stateless gateways holding a shard map (config or a coordination service); sequence numbers, private streams and digests per shard; cancel-all fanned out across shards; cross-shard risk through per-shard budgets | Partitioning, routing, what a global order costs, cross-partition consistency trade-offs |
| 25 | Rebalancing | Move a symbol between shards: snapshot one symbol, hand it over, fence the old owner | Ownership transfer, as in Kafka partitions or database ranges |
| 26 | Kubernetes | A StatefulSet per shard with persistent volumes, anti-affinity across replicas, readiness only after replay, PodDisruptionBudgets; a Helm chart; run on kind or k3d | Stateful workloads on Kubernetes, fsync on network storage |
| 27 | Deterministic simulation testing | The whole cluster in one thread with simulated network, clock and disk, driven by a seed; any failure replays exactly from its seed (the FoundationDB and TigerBeetle approach) | Testing distributed systems exhaustively; the payoff of a deterministic core |
| 28 | Tracing, SLOs, load testing | OpenTelemetry traces from gateway to shard, replication lag, SLOs and alerts (e.g. p99 order acknowledgement); a load generator with HdrHistogram that corrects for coordinated omission; capacity per shard | End-to-end observability, capacity planning |

### Phase 7 — Systems around the exchange (planned)

| # | Step | Scope | Teaches |
|---|---|---|---|
| 29 | Event streaming | Trades and order events published to Kafka or Redpanda; idempotent consumers keyed on sequence numbers; a schema registry | Event-driven architecture, exactly-once effects, schema evolution |
| 30 | Downstream services and post-trade | Positions, P&L and trade history in Postgres as read models (CQRS); a drop-copy feed; end-of-day clearing and a reconciliation job | CQRS, read models, reconciliation |
| 31 | Binary protocols | SBE order entry; market data over UDP multicast with a TCP retransmission server (MoldUDP64-style), which also closes the market-data resend gap | Binary encoding, multicast, gap recovery |

### Open decisions (pick with the user before the step that needs them)

1. **Who writes each step** (§3). The default for new features is the user-writes / LLM-reviews loop.
2. **Runtime dependencies.** The project has none. Each of a metrics library (step 10), a WebSocket server (16), an
   etcd client (22) and a Kafka client (29) would add one; decide per step whether to hand-roll instead.
3. **Java 17 or 21** (step 13: virtual threads need 21).
4. **Own Raft or an etcd lease** (step 22): more learning versus closer to common practice.
5. **Limit scopes with several symbols** (step 11): open-order cap and client order id uniqueness per symbol or per
   participant.
6. **Phase order.** Phase 4 can come before phase 3 for an earlier public demo, provided the journal is kept small
   (step 14, or a scheduled restart that compacts).
7. **Cloud spend. Decided 2026-10-28: zero cost.** Build and run every step on free tooling and free tiers: local
   Docker, kind or k3d for phases 5–7; GitHub Actions and GHCR on a public repository; Oracle Cloud Always Free for
   the demo (step 19). The only accepted cost is an optional domain. Free-tier terms change (Oracle reclaims idle
   instances; capacity is sometimes unavailable), so check the provider's current terms when the step starts.

---

## 8. Decisions confirmed since v3

- **Service layer:** Spring Boot dropped (2026-10-24); plain TCP gateway.
- **Build:** Maven (wrapper included), packages under `exchangeserver.*`, tests in `src/test/java`.
- **Scope expansion (2026-10-27, v5):** at the user's request the roadmap now covers multiple symbols, DevOps,
  distributed systems and a public play-money demo (§7 steps 8–31). The multi-symbol design in step 11 is a
  recommendation, to be confirmed when the step starts.
- **Zero cost (2026-10-28, v6):** the user wants the project built and run for free; open decision 7 is settled
  and steps 8, 9, 14 and 19 name free options (§6, §7).

---

## 9. Glossary

- **Resting order:** sitting on the book (passive side, "maker").
- **Incoming / aggressive order:** being matched now ("taker"); may never rest.
- **Touch:** best bid and best ask levels.
- **Unlink:** splicing a node out of its level's list in O(1) given the node.
- **Sentinel:** dummy head/tail node that removes edge-case branching.
- **Write-ahead:** persisting a command before processing it.
- **Poison pill:** a logged command that crashes the engine and therefore every replay.
- **Quarantine:** a `SKIP` record that makes replay pass over a poison pill without editing history.
- **Smart batching:** the writer takes whatever is already queued, journals it with one fsync, then processes it.
- **Band / collar:** the price range around the last trade outside which priced orders are rejected (band) and
  market orders stop trading (collar).
- **Backpressure:** refusing (BUSY) instead of queueing without limit.
- **State machine replication:** several machines apply the same commands in the same order to deterministic code
  and so reach the same state.
- **Primary-backup:** one replica (the primary) sequences and journals; backups receive its log and replay it.
- **Epoch / fencing:** a number that increases with each new primary; records from an older epoch are refused, so a
  deposed primary cannot keep writing (prevents split brain).
- **Shard:** an independent group (sequencer, journal, replicas) that owns a subset of the symbols.
- **Deterministic simulation testing:** running a distributed system inside one deterministic process with
  simulated network, clock and disk, so every failure reproduces from its random seed.
- **Coordinated omission:** a load-test error where the generator waits for slow replies and so never measures the
  requests that would have queued behind them.
