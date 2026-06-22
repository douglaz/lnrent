# 0002 — Recipes are trusted code; no third-party recipe installation in v1

Recipe hooks (`provision`, `suspend`, `resume`, `destroy`, `healthcheck`) are
executables the daemon runs with high privilege to manage VMs, networking, and the
Box. In v1, recipes are either built into lnrent or authored/vetted by the Operator;
there is no mechanism to install third-party recipes. We chose this because trusting
recipe code by construction is the only safe stance until the core loop is proven:
installing a stranger's recipe would be privileged remote code execution, which
demands signing, review, and sandboxed execution we are not building yet.

## Consequences

- "Extensible" in v1 means the Operator (or the project) can add recipes easily, not
  that buyers or strangers can publish click-installable recipes.
- A signed, curated recipe registry is a possible later direction, not a committed
  milestone. It is its own security project (signing, opt-in trust, sandboxed
  execution) and is explicitly out of v1.
- Recipe execution still follows least privilege (secrets via stdin, not argv/env),
  but recipes are not sandboxed from each other in v1.
