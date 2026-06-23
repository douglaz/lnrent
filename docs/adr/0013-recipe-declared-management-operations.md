# 0013 — Recipe-declared buyer management operations (one ops interface)

A buyer must manage the service they rented, and each service's management surface differs:
WireGuard wants its config/endpoints; a Fedimint guardian wants an admin UI URL + DKG
operations; Hermes wants config + CLI operations; everything wants stop/restart/status. We
will not write a per-service buyer client. Instead, a recipe DECLARES its buyer-facing
operations — the same recipe-driven model as the operator-side lifecycle hooks — and one
buyer interface dispatches them generically.

## Decision

- A recipe declares a **management surface**: named operations `{ name, label, kind, params,
  hook }`. Common ops (`status`, `stop`, `restart`, `get-credentials`) are standard;
  service-specific ops are declared (WireGuard `get-config` / `rotate-key`; Fedimint
  `admin-url` / `dkg-*`; Hermes `get-config` / `set-config` / `exec` / `logs`).
- The buyer client (CLI + web, over buyer-core) **discovers** the operation set and surfaces
  it generically (`lnrent-buyer ops <sub> list` / `<op> [params]`) — no per-service client
  code.
- **Two transports, one interface:** request/response ops (status, restart, get-config,
  admin-url, set-config, dkg-status) ride the NIP-17 DM protocol (`op.request` /
  `op.result`); interactive/streaming ops (shell, console, `logs -f`, file copy, REPL) ride
  the Iroh Native-connect session (§9.2). The client picks the transport by the op's `kind`.
- **Authorization** is by the subscription's buyer: an `op.request` is a NIP-17 DM from the
  buyer's Nostr key, matched to the subscription's `buyer_pubkey`; Iroh sessions are scoped
  by the delivered ticket.
- **Security:** an operation is the recipe's declared *narrow* surface, run by the daemon as
  a recipe management hook — Hermes `exec` is a scoped, declared op, not arbitrary host shell
  (matches the VM guidelines' node-agent narrow-ops rule). AI-free: the hook is
  deterministic, no LLM.

## Considered options

- **Per-service buyer clients / out-of-band tools.** Buyer gets creds and uses some other
  tool. No unified interface; every service a bespoke client. Rejected.
- **A fixed command set in the buyer client.** Doesn't extend to new recipes without client
  changes. Rejected.
- **Recipe-declared operations + one dispatch interface (chosen).** Same recipe-driven
  extensibility as the operator side; new services bring their own ops.

## Consequences

- The recipe manifest gains an `[[operation]]` declaration + a management-hooks dir; the
  recipe runner runs management hooks like lifecycle hooks.
- The DM protocol gains `op.request` / `op.result`; the wire-codec carries them; buyer-core
  owns the `ops` interface (and an Iroh client for interactive kinds).
- M1a proves the request/response op mechanism with the trivial recipe's minimal ops
  (status/restart). Interactive ops over Iroh and the rich per-service ops (WireGuard
  get-config, Fedimint, Hermes) land with those recipes (M1b/M2).
