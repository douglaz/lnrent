# 0015 — Fedimint client uses the RocksDB backend; the daemon ships glibc-dynamic

The Fedimint PaymentBackend (ADR-0012) needs a **fedimint-client** (pinned `0.11.1`, the
version the operator's federation/guardians run) on the control node to receive, hold, and
refund ecash. `fedimint-client` is DB-agnostic — it takes an `IRawDatabase` (a byte-key/value
store with ordered prefix/range scans + atomic transactions) — but the **only published
persistent backend is `fedimint-rocksdb`**. There is no sqlite/redb backend; "use our sqlite"
would mean hand-writing an `IRawDatabase`. RocksDB is C++ and painful to static-link against
musl, which is what surfaced this decision.

## Decision

- Use **`fedimint-client 0.11.1` + `fedimint-rocksdb`** for the Fedimint client's own database.
  The daemon therefore runs **two DB engines**: our **sqlite** store (lnrent state —
  subscriptions / invoices / reservations / op_invocation / …, §11) and fedimint's **RocksDB**
  (the client's internal ecash + federation state). They are separate concerns; we never touch
  the RocksDB directly — the fedimint client owns it.
- The daemon ships **glibc-dynamic** (NixOS is glibc; Debian dynamic is fine). **Fully-static
  musl is dropped as a goal** — RocksDB's C++ on musl is painful and buys little here.
- **wasm is unaffected:** `fedimint-client` is daemon-only. The only crate that compiles to
  wasm32 is `buyer-core` (the web buyer), which — payment being out of scope for the client
  (ADR-0014) — has no payer and no Fedimint dependency at all. The wasm risk lives entirely in
  rust-nostr (the gift-wrap spike), not Fedimint.

## Considered options

- **`fedimint-rocksdb` (chosen).** Blessed, zero adapter code, lowest correctness risk for a
  money database. Cost: a second DB engine in the daemon, and musl-static off the table.
- **Custom sqlite `IRawDatabase`.** Single engine, musl-clean. But front-loaded work
  (~300-line KV-over-sqlite adapter; sqlite orders BLOB keys by memcmp, matching fedimint's
  byte-lexicographic prefix scans) and **we own correctness for the ecash DB** — mitigated by
  fedimint-core's DB **conformance test suite**, which a backend can be validated against.
  **Deferred** as a later optimization (the documented path to single-engine + musl-static),
  not v1.
- **MemDatabase.** Not durable — would lose ecash notes on restart. Rejected.

## Consequences

- Add `fedimint-client` + `fedimint-rocksdb` (`0.11.1`) to the **daemon** crate (a heavy C++
  build); pin to `0.11.1` to match the operator's federation.
- **Backup (lnrent-7fp.14)** must back up the sqlite DB **and** the fedimint RocksDB dir **and**
  the federation invite/config (ADR-0004/0012) — three artifacts, not one.
- **Packaging (§12):** glibc-dynamic; musl-static deferred to a possible later custom backend.
- **lnrent-7fp.4** builds the PaymentBackend on `fedimint-client 0.11.1` with the RocksDB
  backend, backed against a Fedimint test federation in the e2e (lnrent-7fp.15).
