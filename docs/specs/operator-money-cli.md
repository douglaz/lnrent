# Spec: operator money view (`lnrent money`)

Status: **Implemented** (master `5dd2d28`; test `daemon/tests/operator_money_cli.rs`) — a read-only view of the daemon's ecash money position.
*Pending revision (2026-07-04, ledger-authoritative — docs/specs/gate1-alerting-operability.md §E):
the `balance_msat` field and the `BalanceQueryFailed` warning variant below are superseded — plain
`lnrent money` becomes ledger-only (`expected_msat` + earned/reserved/paid-out), and the federation
balance is read solely by the explicit `lnrent reconcile` command. This doc records the landed v1
contract; the §E spec is the forward contract.*
Scope: `daemon/src/{ipc.rs, supervisor.rs, bin/lnrent.rs}` (thread the payment backend into IPC + one
new command). Reuses the INV-2 readiness report; adds NO new money logic.
Audience: the rb-lite implementer. This spec is the contract; the tests below are mandatory ship gates.

## 1. Motivation

The operator CLI (`lnrent`, IPC-over-socket to the daemon) has `status`/`recipes`/`subs`/`sub`, but the
IPC handler is constructed with only `store, recipes, clock` (`ipc.rs::serve` / `dispatch`) — it has NO
access to the payment backend, so an operator cannot see the daemon's **ecash position**: the spendable
balance, whether the gateway is reachable, and whether outstanding refund liabilities are covered. The
INV-2 machinery already computes exactly this (`supervisor.rs::refund_readiness_report(store, payment)` ->
`RefundReadinessReport`), but only logs it at boot/maintenance — it is not queryable on demand.

This adds a `money` command that surfaces that report over IPC, so an operator (or an agent) can check
"can I cover what I owe?" at any time.

## 2. Behaviour contract

- **MONEY-1.** A new `Request::Money` IPC request. The daemon answers with the ecash position, reusing the
  INV-2 readiness for the LIABILITY fields but querying the balance + gateway DIRECTLY so they are always
  shown — the readiness report early-returns a default (`balance_msat=null, gateway_ok=true`) when the
  liability set is empty, and for a money view the operator must still see the real balance + gateway. The
  reply `data` carries exactly:
  - `balance_msat` (int or null): `available_balance_msat()` — null for a backend with no balance concept
    (MockPayment), AND null if the query ERRORS (report unknown, never fail the command); queried
    UNCONDITIONALLY, NOT taken from the report's empty-liability default.
  - `gateway_ok` (bool): `refund_gateway_ready()` — false if the query ERRORS; queried UNCONDITIONALLY.
    (This is the same `Err -> null`/`Err -> false` mapping `refund_readiness_report` itself uses, so
    `money` NEVER fails on a probe error — a probe failure surfaces as `balance_msat:null` /
    `gateway_ok:false`, not an IPC error.)
  - `liability_count`, `gross_liability_sat`, `required_msat`, `parked_count` (ints): from
    `refund_readiness_report(store, payment)` — the SAME liability-scan code path the readiness log uses.
  - `ready` (bool = the report has no warning) and `warning` (string or null — the warning variant name,
    e.g. `"BalanceQueryFailed"`/`"GatewayUnavailable"`/`"InsufficientBalance"`/`"Unpriceable"`/
    `"ParkedManual"`, or null when covered): the liability-gated readiness verdict. NOTE: with zero
    liabilities `ready` is `true` even if `gateway_ok` is false or the balance is 0 — that is correct
    (nothing is owed, so nothing is at risk); the raw `gateway_ok`/`balance_msat` still surface the fact.
- **MONEY-2 (read-only, no side effects).** `money` MUST NOT mint, pay, mutate state, or move ecash. It is
  a pure query — a balance/gateway read + the store liability scan the readiness report already performs.
- **MONEY-3 (mock/default is fine).** Under the default MockPayment (no fedimint), `available_balance_msat`
  is `None` and `refund_gateway_ready` is `true`; `money` reports `balance_msat: null, gateway_ok: true`
  and the liability counts from the store, `ready: true` when nothing is owed. It never errors just
  because there is no real ecash backend.

## 3. Wiring

- Thread `payment: Arc<dyn PaymentBackend>` through the IPC serve chain: `serve`, `serve_with_shutdown`,
  `handle_conn`, `dispatch` all gain the param (the supervisor already holds `self.payment` and calls
  `ipc::serve_with_shutdown(...)` at `supervisor.rs:439` — pass `self.payment.clone()` there). Update every
  caller (the supervisor + any IPC tests) to the new signature.
- Make `refund_readiness_report` and `RefundReadinessReport` (and `RefundReadinessWarning`) reachable from
  `ipc.rs` — `pub(crate)` in `supervisor.rs`, or move the report type + fn to a small shared module. Add a
  method/`From` to turn a `RefundReadinessReport` into the MONEY-1 `serde_json::Value` (the `ready` bool =
  `warning.is_none()`; `warning` = the variant name or null). Keep the reservation-liability semantics
  exactly as INV-2 defined them.
- `dispatch(Request::Money)`: query `payment.available_balance_msat()` + `payment.refund_gateway_ready()`
  DIRECTLY (with the MONEY-1 `Err -> null`/`Err -> false` mapping), AND call
  `refund_readiness_report(store, payment).await` for the liability fields + `ready`/`warning`. Build
  `report_json` from the report but OVERRIDE `balance_msat` + `gateway_ok` with the direct-query values, so
  they are correct even on the report's empty-liability default path. Only an `Err` from
  `refund_readiness_report` itself (a store/scan failure) becomes `Reply::err("internal", …)`; the direct
  balance/gateway probe errors do NOT fail the command (they map to null/false per MONEY-1).

## 4. CLI

`bin/lnrent.rs`: add `Money` to the `Cmd` enum (help: "Show the daemon's ecash position: balance, gateway,
and refund-liability coverage"). It sends `Request::Money`. Under `--json` it emits the FULL
`{ "ok": true, "data": { …MONEY-1 fields… } }` Reply envelope, exactly like the other commands (NOT the raw
`data` alone). In human mode it prints one line each for balance, gateway, outstanding liabilities (gross +
required), parked count, and a bold `READY` / `NOT READY (<warning>)`. Exit codes follow the shared
IPC/`not_found`/error taxonomy (0 on ok).

## 5. Tests (mandatory)

- MONEY-covered: a daemon with zero liabilities (fresh store) → `money` returns `ready: true`, `warning:
  null`, `liability_count: 0`; under MockPayment `balance_msat: null`.
- MONEY-covered-real-backend (proves the unconditional direct query): zero liabilities + a payment double
  reporting a CONCRETE `available_balance_msat` (e.g. 12_345) and `refund_gateway_ready = false` → `money`
  returns `balance_msat: 12345` and `gateway_ok: false` (NOT the report's empty-liability default
  null/true), with `ready: true` (nothing owed). This catches an implementation that dropped the
  unconditional direct query and just serialized the report.
- MONEY-liability: seed an uncovered liability (a PENDING refund the balance can't meet, via a payment
  double that reports a low `available_balance_msat` + the store liability) → `money` returns `ready:
  false`, the matching `warning`, and the non-zero counts. (Mirror the INV-2 `refund_readiness_report`
  supervisor tests' setup.)
- MONEY-readonly: `money` performs no writes (assert the store is unchanged across the call) and moves no
  ecash. A recording payment double sees ONLY read-only calls — `available_balance_msat`,
  `refund_gateway_ready`, `refund_required_outlay_msat`, and the in-flight-exclusion probes
  `payment_status_by_key` / `payment_started_by_key` the readiness path uses — and NEVER
  `create_invoice` / `pay` / `pay_refund_capped` or any mutation.
- IPC-signature: the existing IPC tests still pass with the threaded `payment` param (use a MockPayment).
- CLI envelope: `lnrent --json money` emits a `{ "ok": true, "data": { … } }` envelope with the MONEY-1
  fields and exit 0 (mirror the existing `status` command's CLI test).

## 6. Non-goals

- No funding / receiving ecash (`fund`): the daemon self-funds refunds from received sales, and an explicit
  fund-payment would trip the auto-refund of unmatched settlements — out of scope here; if added later it
  needs a receive path the settlement handler recognizes as non-order.
- No new money computation: `money` is exactly the INV-2 readiness report, surfaced on demand.
- No per-subscription money breakdown, ledger export, or historical view (a single current snapshot).
- No mutation of the payment backend or any ecash movement.
