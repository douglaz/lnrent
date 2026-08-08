# 0021 — No blocking work on a runtime worker thread

**Status: DECIDED, NOT YET BUILT.** Nothing described here is in the tree at the time of
writing. `Store::spawn` still mounts the actor with `tokio::spawn` (`daemon/src/store.rs`),
both backend indexes are still raw `Arc<Mutex<Connection>>` called from async fns, `lnrentd`
still depends on `rusqlite` directly, and there is no `clippy.toml`. Read every "does" and
"is" below as "will" until the delivery beads close. That set is derivable rather than listed
here, so it cannot go stale as beads split or get added:

```bash
br list --limit 0 --json -a | jq -r '.[] | select(.status!="closed")
  | select((.description // "") + (.title // "") | test("ADR-0021")) | "\(.id) \(.title)"'
```

`lnrentd` is a tokio application whose two most important dependencies are synchronous:
`rusqlite` (a C binding, with no async form) and the filesystem. This ADR fixes how those
are reached, because the alternative is the failure mode where a later edit adds a
blocking call to an async path and nothing catches it.

**The rule: once the daemon is serving, no blocking I/O and no unbounded-duration work runs
on a tokio worker thread. Blocking I/O resources are reachable only through an async API.**

Earlier drafts said "no blocking call", which review correctly rejected three times as a
promise this ADR does not keep. What matters is work whose duration is unbounded or scales
with data: sqlite queries, file I/O, sleeps, network calls.

**In-memory `std::sync::Mutex` sections are a named exception, not an instance of the rule.**
Holding one is the recommended tokio practice for short critical sections, and swapping
`relay_status.rs` and the Nostr engine's trackers for `tokio::sync::Mutex` would be slower and buy
nothing.

`alerts.rs` was on that list and is no longer (lnrent-gc7). Its cooldown is not a short critical
section over data — it is a check-then-commit across an `await`: read `last_sent`, enqueue the DM,
then stamp. With a `std::sync::Mutex` the guard cannot be held across the enqueue, so two
concurrent observations of the same `(kind, subject)` both read "not sent recently" and both
enqueue, which duplicates the alert the cooldown exists to suppress. It now uses a
`tokio::sync::Mutex` held across the await deliberately — the exception traded for correctness, not
performance. The section still does I/O (the outbox insert) while holding it, so it is bounded by
one enqueue and by nothing else; that is a known cost, recorded here rather than left implicit. But do not claim a duration bound for them: a `std::sync::Mutex` has
none when contended or when its holder is descheduled.

The exception carries constraints rather than a guarantee: inside the critical section, no I/O,
no unbounded iteration, and nothing that grows with request volume. **Those constraints bind new
code; three sections in the tree today do not meet them**, and saying so is the point — an
exception list that quietly includes its own counterexamples is a false contract:

- `relay_status.rs:77` clones the whole relay vector under the lock.
- `alerts.rs` inserts into an unbounded `last_sent` map keyed by `(kind, subject)` — now under
  the `tokio::sync::Mutex` described above, so the unbounded-growth constraint still applies while
  the duration one no longer does.
- `nostr_engine.rs:1839` does a `retain()` scan over a vector sized by in-flight requests.

No delivery bead changes them, and none of the three is a live hazard at current
scale. `lnrent-njv`'s audit assesses each and either bounds it or records why it is tolerated;
until then they are grandfathered, not conforming. Sections that outgrow the constraints stop
being exceptions and come back into scope.

The genuinely dangerous mutex case — a guard held across an `.await` — is separately and
already denied by `clippy::await_holding_lock` under CI's `-D warnings`.

Two scope words in the rule are load-bearing, because an absolute phrasing would be a claim
this ADR does not deliver.

*"Once serving"* — the exempt phase is **pre-serving**, not "before the runtime". Only
`config::load_raw_config` (`main.rs:243`) precedes `build_runtime` at `:254`; everything else
called "startup" — `config::prepare_data_dir` (`main.rs:410`),
`config::bootstrap_headless_with_store` (`:415`), `load_operator_recipe` (`:451`) — runs inside
async `run_daemon`, on a worker, after the store actor already exists. What makes them exempt is
not the absence of a runtime but that **no request has been accepted yet**, so blocking there
delays only startup.

**This ADR gives the criterion, not the classification.** Two things decide a site:

- *Fixed-cost and pre-serving* → exemptible with a reason.
- *Recurs, scales with data, or can run once requests arrive* → in scope, offload it.

Four review rounds each produced a different wrong classification from earlier drafts of this
section — `load_operator_recipe` filed as bounded when `Recipe::load_all` (`recipe.rs:153-166`)
walks `read_dir` unbounded; the IPC bind filed as startup when `supervisor.rs:927` rebinds on a
live daemon; `bootstrap_headless_with_store` filed as fixed-cost when it reaches
`read_secret_file_bytes`'s unbounded `read_to_end` (`config.rs:1746`). The pattern is the point:
a decision record cannot hold an accurate site-by-site taxonomy of a moving codebase.
**`lnrent-njv` owns the classification**, applying the criterion above, deriving the sites rather
than inheriting a list from here. The three misfilings above are recorded there as known
inputs — not as a complete set.

**The IPC socket bind deserves a specific warning**, because it looks like startup and is not.
`ipc.rs:406`'s
`bind_owner_only` is reached through `serve_with_shutdown`, which `supervisor.rs:927-932`
restarts in a backoff loop — so a listener error rebinds while Nostr and maintenance stay
live. It is recurring work on a live daemon and belongs in the offload set. An earlier draft
of this ADR filed it under startup and was wrong.

One narrow exception is deliberate and named here so the rule and the beads cannot disagree:
**the graceful-shutdown unlink.** `ipc.rs:471` removes the socket before in-flight handlers
drain at `:487`, so it does run while requests are in flight. It stays blocking, on a path
already committed to shutting down, where there is no latency left to protect.

Do not call it *bounded*, though — an earlier draft did, and that was wrong. `std::fs::
remove_file` on a wedged or networked filesystem can block indefinitely, and running
synchronously on a worker it cannot observe cancellation, so this path can outlast
`SHUTDOWN_GRACE`. That residual is **accepted, not eliminated**: the socket lives in the data
dir, so a filesystem wedged enough to hang this unlink has already made the daemon unable to
commit anything, and adding an offload here buys a clean exit for a process that is
unrecoverable regardless. Stated rather than papered over — the whole point of this ADR is
that a claimed bound nobody checked is worse than an acknowledged gap.

*"Blocking"* — this is not "all code is async", which is unachievable: sqlite has no async
implementation in Rust. Every crate advertising one (`tokio-rusqlite`, `deadpool-sqlite`,
`sqlx`'s sqlite driver) is a thread-and-channel wrapper around the same blocking calls. The
achievable rule is about *where* blocking runs, not whether it exists.

## The sanctioned pattern

One shape, to be used for all three sqlite connections (the state DB, the lnv2 index, the
phoenixd index): a dedicated OS thread owns the `Connection` and receives closures over a
**bounded** channel; the **serving** surface is `async` only. Not *every* surface — the crate
also exposes one deliberate synchronous door for the offline backup CLI (see Enforcement).
Today only the state DB has anything resembling this, and it is mounted wrongly; the indexes
have no boundary at all.

```rust
// Shape only. The connection is OPENED ON THIS THREAD, not handed in:
// Connection::open + PRAGMAs + quick_check + migrations are blocking too.
let (tx, mut rx) = mpsc::channel::<Job>(64);
std::thread::spawn(move || {
    let mut conn = match open(&path) { Ok(c) => c, Err(e) => { let _ = ready.send(Err(e)); return } };
    let _ = ready.send(Ok(()));            // initialisation result, back to the caller
    while let Some(job) = rx.blocking_recv() {
        job(&mut conn);
    }
});
```

### Shutdown is a requirement here, not a mechanism

Graceful shutdown must satisfy three things at once, and this ADR states them as obligations
because **three successive drafts specified a mechanism and all three were wrong** — the record
is worth more than another guess:

1. **On a normal drain, enqueued work runs to completion.** The guarantee starts at *successful
   enqueue*: `Store::run` returns from `tx.send(job).await` with `Ok`, then awaits a oneshot, so
   a caller cancelled at that second await leaves a job in the queue that must still run. A
   caller cancelled *before* the send completes has no such job and is owed nothing. Closing the
   senders lets `blocking_recv` return `None` once the queue drains.
2. **Shutdown stays bounded, and outranks obligation 1 when they conflict.** `SHUTDOWN_GRACE`
   (`supervisor.rs:95`) exists so "a stuck loop must not hang process exit". A stalled fsync
   must not outrank that.
3. **The wait itself obeys this ADR.** It cannot block a tokio worker for an unbounded time.

Obligations 1 and 2 *do* collide, and the resolution is stated rather than left to the
implementer: when one job stalls, everything enqueued behind it is **abandoned, deliberately and
loudly** — obligation 1 describes the normal drain, not the timeout path. Exiting on the bound
is the correct behaviour, and the timeout is an acknowledged data-loss event: log the abandoned
queue depth so it is visible in the operator's logs rather than inferred. WAL plus
`synchronous=FULL` gives SQLite's documented commit durability *under its filesystem
assumptions* — an interrupted job leaves the DB consistent; it says nothing about the queue
having been empty, and nothing about I/O, filesystem or storage failure (that is what
`lnrent-y4m.3`'s degraded-mode latch exists for). `lnrent-skk` must test exactly this shape — a
healthy job enqueued *behind* a stalled one — because that is where the two obligations meet.

Rejected drafts, each defeated by the next review round: a bare `handle.join()` (unbounded, and
blocks a worker — breaks 2 and 3); `timeout(spawn_blocking(|| handle.join()))` (a timed-out
blocking task cannot be abandoned, and dropping the runtime at `main.rs:258` then waits for it,
so the process still hangs — breaks 2). A completion **signal** the shutdown path can await and
abandon looks more promising than any join, since a detached thread does not hold up process
exit — but `lnrent-skk` owns choosing and, above all, **testing** it. The stalled-job path is
the test that matters; without it this is three guesses and a fourth.

WAL plus `synchronous=FULL` means an interrupted job leaves the database consistent, within
SQLite's documented filesystem assumptions — not that storage cannot fail under it. What an
abandoned queue does do is drop work a caller was told had been accepted, which is why
obligation 1 exists for the normal path and why the timeout path has to announce itself rather
than pass quietly.

`std::thread::spawn`, not `tokio::spawn` — the distinction is the whole point. The actor
already exists in this shape and is mounted on a tokio task, which means SQL occupies a
runtime worker for its duration.

## Considered options

- **`tokio-rusqlite`.** Its implementation (`src/lib.rs:376`) is
  `crossbeam_channel::unbounded` + `thread::spawn` + a closure event loop — the same design
  adopted above, which is the strongest argument that the design is right. Rejected as a
  *dependency* for three reasons. It requires `rusqlite ^0.37` against our `0.31`, and
  `libsqlite3-sys` declares `links = "sqlite3"`, so cargo forbids coexistence: adopting the
  crate *forces* a rusqlite major bump as a side effect of a concurrency fix. Its channel is
  unbounded, where `mpsc::channel(64)` makes the caller await — so on a `synchronous=FULL` money
  DB a slow fsync becomes visible pressure at the call site instead of a silently growing queue.
  (That bounds the ACTOR's queue and propagates pressure to the caller; it does not bound total
  in-flight memory — see the consequences.)
  And `transaction()`, the degraded-latch (ADR-0001, lnrent-y4m.3) and the sole-writer
  contract are built on top either way, so the code it replaces is the dozen lines that
  spawn the thread. **Take the design, not the dependency.**
- **`spawn_blocking` per job.** Moves work off the workers without a new thread. It does
  **not** dissolve serialization — an actor loop that awaits each job before receiving the
  next stays strictly sequential — so it is rejected on cost, not on correctness: `Connection`
  is not `Sync`, so each job must move the connection into the closure and recover it from the
  `JoinHandle`, paying that shuffle plus a blocking-pool dispatch on every single query. One
  long-lived thread does the same work with neither.
- **A read pool alongside the writer.** WAL permits concurrent readers, so this is a real
  throughput gain — rejected because it introduces a second concurrency model into the money
  core to solve a performance problem nobody has measured.
- **Converting the filesystem calls to `tokio::fs` wholesale.** Rejected: the bulk of the
  daemon's filesystem work is **pre-serving** — of `config.rs`'s three entry points only
  `load_raw_config` is genuinely pre-runtime, while `prepare_data_dir` and
  `bootstrap_headless_with_store` run inside `run_daemon` before any request is accepted (see
  the table above) — or offline (`backup.rs`, which refuses to run against a live daemon).
  Converting those buys nothing.

  **This ADR deliberately does not carry the audit of which sites are exempt.** Four review
  rounds each found another live blocking path an earlier draft's "exhaustive" classification
  had missed — the shutdown unlink at `ipc.rs:471`, the supervised IPC rebind
  (`supervisor.rs:927` loops and re-invokes the factory, so `bind_owner_only` recurs during
  recovery, not just at startup), and `hook.exists()` on a live `Preflight` request
  (`preflight.rs:650`), which is blocking metadata I/O that matches neither an `fs::` grep nor
  a `std::fs` denylist. A decision record that also claims a finished inventory is a record
  with a rotting half. **The audit is `lnrent-njv`'s deliverable**, derived rather than
  recalled; this section records only the decision not to convert wholesale.

## Enforcement

Prose does not hold a rule like this; the rule is only real if a violation fails a build.
Two mechanisms, because neither is sufficient alone:

- **A crate boundary.** The sqlite layer moves into its own crate and `lnrentd`'s manifest
  drops `rusqlite` (`lnrent-7dw`). "Only async APIs" is nearly true and the exception is
  structural: `backup.rs` opens the state DB directly for `VACUUM INTO` (`backup.rs:619`, via
  the `rusqlite::Connection` import at `:66`) and is an offline CLI that refuses to run
  against a live daemon, so it needs a
  *synchronous* offline entry point from the same crate. That is compatible with the rule —
  it never runs while serving — but the crate exposes one deliberate sync door, and pretending
  otherwise would leave the binary uncompilable the moment the dependency is dropped.
- **A `clippy.toml` denylist.** `disallowed-methods` covering `rusqlite::Connection::open`
  and the blocking primitives, denied by CI's `-D warnings`.

**Withholding `Connection` from the re-export is NOT sufficient, and the reason is worth
recording so nobody re-proposes it.** The obvious design — re-export `Transaction`, `Row` and
`params!`, withhold `Connection`, and rely on `Connection::open` being an associated function
that must be named — is bypassable through an associated-type projection. Measured on rustc
1.96.0 in a three-crate build where the consuming crate had **no dependency on the sqlite
crate at all**:

```rust
type C = <store::Transaction<'static> as std::ops::Deref>::Target;
let _ = C::open("/tmp/x.db");
let _ = C::open_in_memory_with_flags(Default::default());
```

`Transaction`'s `Deref<Target = Connection>` (rusqlite 0.31 `src/transaction.rs:232`) hands
out the type itself, not merely method access, so every `Connection` constructor is reachable
without naming or depending on `rusqlite`.

The probe that established this used a stand-in crate whose flags argument was a `u32`; real
rusqlite takes `OpenFlags`, hence `Default::default()` above. That detail is not pedantry —
it is the whole hazard of the compile-fail fixture `lnrent-7dw` requires. A fixture that fails
to compile because the *argument types* are wrong proves nothing about the boundary while
happily reporting success. **The fixture must assert on the compiler's reason** (an
unresolved/sealed type), not merely on failure.

The boundary therefore has to be a type that does **not** `Deref` to `Connection`: the store
crate exposes its own `Txn` newtype wrapping `rusqlite::Transaction` privately and forwarding
the query methods callers actually use. No projection path, no bypass. Every SQL site in the
daemon keeps its closures and business logic and simply calls the same methods on a different
receiver. To size that before starting, derive the set rather than trusting a number written
here:

```bash
# Qualified paths AND the unqualified uses that follow `use rusqlite::…`, which a
# `rusqlite::`-anchored pattern misses entirely (backup.rs:619 is exactly that case).
rg -n '\b(Connection|Transaction|OpenFlags)\b|params!|\.query_row\(|\.prepare\(' daemon/src --stats
```

Treat that as a **lower bound for sizing, not proof of coverage** — a text pattern cannot be
shown complete, and four separate derivation commands in this ADR's review history were each
wrong in a different way. If completeness matters, use an AST-based inventory.

A typed repository API (one method per query, owning every site) is still **not** required
for enforcement — the newtype gets there. That refactor has its own merit, a single audited
SQL surface, and should be decided on that merit rather than smuggled in as the price of this
rule.

The denylist then covers what no crate boundary can: `std::thread::sleep`, blocking
filesystem entry points, and any other explicitly listed blocking primitive from crates the daemon legitimately
depends on. An explicit `#[allow(clippy::disallowed_methods)]` is the marker of a deliberate,
sanctioned boundary — it should be rare and greppable.

**What this enforcement does and does not buy.** `disallowed-methods` matches the exact paths
listed in `clippy.toml` and nothing else: an unlisted blocking call (`std::net::TcpStream::
connect`, `std::sync::mpsc::Receiver::recv`) still compiles silently. Demonstrating one listed
violation proves the lint is *live*, not that every violation fails the build. So the honest
contract is: the sqlite path is closed structurally, an audited set of named primitives is
closed by lint, and everything else rests on review. Claiming more than that would be the same
error as the bypass above — asserting a barrier without testing what walks through it.

## Consequences

- Blocking sqlite runs on threads outside the runtime's control, so a slow fsync delays the
  store's queue rather than starving unrelated tasks.
- The bounded channel makes store backpressure observable and bounds the actor's own queue: a
  caller awaits `tx.send` instead of enqueueing into a queue that only grows. It does **not**
  bound total pending memory, and saying it did would be the kind of unchecked claim this ADR
  exists to prevent — `serve_with_shutdown` accepts every same-UID connection and spawns a
  handler with no semaphore (`ipc.rs:424-449`), so excess work parks in an unbounded set of
  waiting tasks, each holding its `Job`. Capping IPC admission is a separate question and
  deliberately not opened here: the socket is owner-only and `SO_PEERCRED`-gated, so anyone who
  can flood it already has the operator's privileges, which makes this a robustness limit worth
  stating rather than a security boundary worth building.
- Total serialization through one actor is unchanged. The money path depends on it
  (lnrent-26b preserved money-path serialization by *sharing* the actor), and this ADR does
  not relax it.
- `clippy::await_holding_lock` is warn-by-default and already denied by CI's `-D warnings`,
  so the narrowest and most dangerous version of this mistake — a `MutexGuard` held across
  an `.await` — was already a hard gate before this ADR.
- New blocking **sqlite** work needs either an actor or an explicit `#[allow]`. New blocking
  work of other kinds needs review to catch it unless its exact path is on the denylist — see
  the enforcement section. This ADR closes one class structurally and an audited list by lint;
  it does not close the category.
