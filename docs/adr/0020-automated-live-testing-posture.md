# 0020 — Automated live testing: a Synthetic Buyer on compressed time

The live money path was proven once, by hand: lnrent-7qc carried a single mainnet order
through the lnv2 backend, and the phoenixd API contract was measured interactively against
the staging node. Neither is repeatable without the author. Every TIME-DRIVEN transition —
the six reconcile due-scan arms that a real Buyer meets on day 30 rather than day 0 — has
never run against real money at all. Operator directive (2026-07-31): remove the human from
the loop.

## Decision

- **A Synthetic Buyer is permanent infrastructure**, not a fixture. It gets its own Nostr
  identity and its own **dedicated phoenixd node**, stood up now rather than when phase 2
  forces it. Buyer and Operator money stay strictly separate: the phase-1 Operator backend
  is fedimint, but phase 2 makes phoenixd the Operator backend too, and a node cannot
  meaningfully pay an invoice it issued.
- **The unattended run executes as a k8s CronJob in-cluster** (hetzner, via the ArgoCD
  GitOps repo), because that is the only place that reaches the federation, both phoenixd
  nodes, and a DO token at once. There are no self-hosted GitHub runners, and a
  GitHub-hosted runner can reach none of it.
- **Blast radius is bounded by construction, not by code.** The Synthetic Buyer's wallet
  holds roughly three runs' worth, so a runaway drains a deliberately small wallet and
  stops; and a reaper sweeps EVERY droplet carrying the test tag at job START, so a
  SIGKILLed run is cleaned by the next one rather than by a `Drop` impl that never fires.
- **Time is compressed and nothing runs at real cadence.** `period=10m / renew_lead=5m /
  retention=5m`. A run is ~90 minutes, floored by the fixed one-hour order-invoice expiry.
- **Failure reaches the operator by Nostr DM through the real `OperatorAlert` path**,
  backed by a fate-independent cluster-level signal on job exit. The primary path
  deliberately dogfoods alert delivery; the backstop exists because a daemon broken enough
  to fail the run may be too broken to send a DM.
- **Only the MECHANICAL half of the dogfood is automated.** Whether the operator surface
  MISLEADS a stranger is not something a machine can report. lnrent-9q9's judgment half is
  explicitly DEFERRED, not deleted — see Consequences.

## Considered options

- **Real cadence, or a long-lived canary subscription at `period=30d` (rejected).** A
  single always-on subscription would have cost one droplet and proven what compression
  cannot. Rejected by the operator in favour of accepting the gaps: the mechanical arms are
  the point, and a month-long feedback loop reports regressions a month late.
- **Hard spend/droplet caps in code that abort the run (rejected).** The strongest in-run
  stop, but it is new money-path code that would itself need proving, and a false trip
  costs a night's signal. Bounding by wallet size needs no code to be trusted.
- **A fedimint client as the Synthetic Buyer (rejected).** Cheapest in fees, but Buyer and
  Operator would share a federation and the payment might never traverse the gateway or a
  real Lightning hop — most of what phase 1 exists to prove.
- **Run on the author's box, or on a self-hosted GitHub runner (rejected).** The box is not
  project infrastructure. The runner was the cheaper build and would also have unblocked
  the already-written `fedimint-live.yml`; the cluster was chosen for durability instead.

## Consequences

- **Packaging becomes a prerequisite.** A CronJob needs an image and `flake.nix` exposes
  only a devshell — no `packages.*`, no Dockerfile. A nix package for the binaries is the
  shared prerequisite of this work AND lnrent-ea1. It is NOT the same deliverable: ea1 is
  an operator-facing release (portable binaries, systemd unit, install doc for a stranger
  on a VPS); this needs an internal container image. Build the package once, feed both.
- **These gaps stay unproven, knowingly:** daemon survival across weeks; DigitalOcean
  billing over a real month; a real 30-day BOLT11 expiry (the compressed run never mints
  one); fedimint ecash sitting at rest that long; and any failure whose period is longer
  than ninety minutes. Anything asserted about lnrent at 30-day timescales rests on
  reasoning, not evidence. This is the ADR's least comfortable clause and the one most
  likely to be revisited.
- **The judgment half of lnrent-9q9 is deferred, not closed.** A green nightly run says the
  arms fire; it says nothing about whether the docs mislead or the CLI confuses. Since
  CONTEXT.md's thesis is an ecosystem of third-party Operators, that signal is product
  surface. Deferring it by default is the risk this decision takes on: the run going green
  can read as "phase 1 is done" when the operability question was never asked.
- **`INVOICE_EXPIRY_S` stays a constant.** Making it configurable would shorten runs, but
  it is deliberately "not an operator config knob" and the test should not wag the money
  path. The one-hour floor is accepted instead.
- The Synthetic Buyer spends real mainnet sats nightly, forever. That is a standing
  operational cost, and its wallet needs periodic refunding — a chore this ADR creates and
  lnrent-rwv's accounting discipline must cover.
