# 0014 — AI-free control plane, agent-native via a complete CLI

We expect a majority of marketplace activity (~51%) to be intermediated by AI agents on
BOTH sides — operator agents that offer and manage services, and buyer agents that rent and
control them — with humans (~49%) still first-class. This looks like it conflicts with the
hard **AI-free control plane** invariant (§4.1, ADR-0001). It does not: they are different
boundaries.

## Decision

- **Two boundaries, complementary.** "AI-free control plane" is about what runs *inside* the
  operator's serving / trust boundary (no LLM in the daemon's payment → provision → lifecycle
  path). "Agent-native" is about who *drives* the system from *outside* (agents and humans).
  Keeping LLMs out of the serving path is exactly what makes heavy agent mediation safe — it
  is the prompt-injection firewall, not a barrier to agents.
- **The agent surface is a complete CLI. No MCP, no HTTP server.** Both sides are operable by
  agents through the `lnrent` CLI (operator) and the buyer CLI (a buyer-core front-end). A
  complete CLI is sufficient for any agent; we do NOT ship an MCP server or a local HTTP API —
  both reintroduce a server, auth, lifecycle, and a schema surface a good CLI already covers,
  and an HTTP API on the buyer side would break the static no-central-server web property
  (§1). Anyone who wants HTTP can wrap the `--json` CLI themselves.
- **CLI-completeness contract** (both CLIs): every operation reachable from the CLI; `--json`
  machine-readable output on every command; fully non-interactive (flags / env / stdin —
  never a *required* prompt); deterministic exit codes + structured errors; scriptable
  discovery (`list` / `describe` emit JSON). The web client stays the HUMAN surface (static
  SPA: NIP-07 / WebLN / QR), not an agent API.
- **Payment is out of scope for the client.** The CLI / buyer-core is a protocol client, not
  a wallet: it **returns the invoice** (bolt11 + amount + expiry, as structured output) and
  never holds funds or pays. Paying is the buyer's wallet's job, out-of-band — a human pays
  from their wallet, an agent pays with its own payment logic — after which the operator
  confirms settlement and the flow resumes. The web SPA may offer a human WebLN / QR hand-off,
  but that is the user's wallet paying, not the client.
- **Dual-side injection threat model.** In an agent-mediated marketplace, untrusted content
  flows *into* the client agents: a buyer agent reads listings, DMs, and op.result / provision
  payloads; an operator agent reads order / op params and DM content. The operator side is
  already protected by the AI-free plane (deterministic validators, never an LLM). The buyer
  side applies two disciplines (§13): **provenance** — act only on signature-verified fields,
  verified milestone-aware (M1a: the operator's account-0 signature only; M5+: operator
  manifest ADR-0006 + attestations ADR-0011); and **taint** — a signature proves provenance,
  NOT safety, so operator-produced payloads (`provision.ready.payload`, hook delivery output,
  `op.result.data`) are untrusted as instructions: buyer-core separates typed known fields
  from opaque/display-only blobs and free-text, and never auto-executes a URL or command from
  any payload.

## Considered options

- **MCP server / agent SDK.** Rejected — a server + protocol + schema-maintenance surface
  that a complete CLI already covers; the operating stance is "a CLI is enough for any agent."
- **Local HTTP API on the buyer / web side.** Rejected — reintroduces a server (the thing the
  CLI avoids) and breaks the static no-central-server web property (§1). Wrapping the CLI in
  HTTP is a trivial external concern if ever needed.
- **Embed an LLM to "understand" requests.** Rejected — violates the AI-free invariant and
  opens prompt injection into the value / hosting plane.

## Consequences

- Both CLIs are built `--json` + non-interactive from the start (cheap by design, expensive
  to retrofit): buyer CLI (lnrent-7fp.13), operator CLI ↔ daemon surface (lnrent-7fp.12),
  operator bootstrap (lnrent-7fp.16).
- `order.error` gains a structured `code` (like `op.result`), so agents branch on outcomes.
- The client never pays: it returns the invoice and the buyer's wallet settles it out-of-band.
  M1d proves a fully-headless agent loop where the buyer agent's own wallet pays — no payer in
  the client, no MCP, no server.
- The web buyer (lnrent-7fp.18) is explicitly the human surface; it is not an agent API.
- The dual-side injection threat model is recorded in §13; buyer-core enforces
  signed / structured-vs-prose separation.
