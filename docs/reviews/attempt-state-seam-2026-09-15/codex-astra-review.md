FAIL — the attempt-state recommendation at `7927bfb` conflates payment evidence, refund obligations, and sweep reservations.

[P1] daemon/src/ledger.rs:69, daemon/src/ledger.rs:202, daemon/src/sweep.rs:133 — An unfenced PENDING refund with Unknown/not-started backend evidence subtracts nothing from expected holdings; a PENDING sweep subtracts its cap immediately. Surplus nevertheless reserves that refund. Define separate accounting predicates before introducing “committed.” Kind can parameterize accounting policy; it cannot make these different quantities one concept.

[P2] daemon/src/refund.rs:668, daemon/src/refund.rs:868, daemon/src/legacy_import.rs:792, daemon/src/legacy_import.rs:823 — One refund row can advance through payment generations; a sweep’s payment key is its row ID. Decide whether Attempt identifies the enduring obligation or one payment generation. Key derivation is a parameter; obligation versus payment generation is a different concept. Also, “receipt gross” misstates the refund bound: provenance uses floored wallet credit (`daemon/src/refund.rs:775,798`).

[P2] daemon/src/store.rs:1284, daemon/src/supervisor.rs:1808, daemon/src/supervisor.rs:1859 — The omitted supervisor reader exposes existing drift: readiness drops the fence, counts only FAILED refunds as parked, and prices fenced PENDING refunds normally. With sufficient expected holdings and healthy probes, it can report no warning despite the driver refusing execution (`daemon/src/refund.rs:340`). Include this consumer and define its fence result explicitly.

[P3] daemon/src/store.rs:1103, daemon/src/store.rs:1209, daemon/src/legacy_import.rs:843, daemon/src/backup.rs:180 — The inventory also misses retention guards and restore admission. They interpret attempt status to preserve refund references or require correlation evidence; include them in the consumer audit.

The prior question is: **which facts and decisions must remain consistent?** The fence is already orthogonal to lifecycle: clearance changes only the stamp (`daemon/src/ipc.rs:1280`), as ADR-0022 explicitly requires (`docs/adr/0022-backend-state-lives-in-the-books.md:430`).

A shared payment-evidence reader is defensible. The recommended claim that kinds differ *only* in purpose and amount is not. Keep refund obligation and sweep authorization distinct, with explicit projections from common evidence.

Required invariants:

- Identify the current payment key and destination; reject conflicting correlation evidence (`daemon/src/legacy_import.rs:989`).
- Distinguish reserved liability, expected-holdings deduction, and additional liquidity required—the accounting findings above.
- Separate permission to prepare/send, re-await, replace a destination, and release a cap (`daemon/src/refund.rs:900,662`; `daemon/src/sweep.rs:690`).
- Preserve sweep-slot exclusion and recovery’s self-cap exclusion (`daemon/src/sweep.rs:859,178`).
- Separate execution selection from operator attention: both drivers select only PENDING, while fence views include other statuses (`daemon/src/refund.rs:1258`; `daemon/src/sweep.rs:1072,1195`).

“Parked” should describe an action restriction with a reason and remedy, rather than collapse those distinctions into another lifecycle value.