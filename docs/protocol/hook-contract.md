# Recipe hook contract

**Status:** normative. Extracted from `daemon/src/runner.rs`, `provision.rs`, `reconcile.rs`,
`resume.rs`, `op_dispatch.rs`, `preflight.rs` and `recipe.rs` on 2026-09-09.

A recipe is a directory the daemon runs hooks from. Hooks are plain executables in any
language. The daemon never special-cases a service: it reads the manifest, runs the hooks,
and stores what they return.

```
<recipe-dir>/
  recipe.toml        # manifest, §1
  provision          # required lifecycle hooks, §3
  suspend
  resume
  destroy
  healthcheck        # required by validation; not invoked by the daemon today
  preflight          # optional, §3.5
  ops/<hook>         # one per declared operation, §3.4
```

## 1. Manifest `recipe.toml`

```toml
[service]
id = "do-vps"                 # non-empty; becomes the listing `d` tag
name = "Cloud VPS (DigitalOcean)"
summary = "…"
version = "0.1.0"
category = ["vps", "compute"] # optional

[pricing]
amount_sat = 30000
period = "30d"                # duration grammar: listing.md §3
renew_lead = "7d"
retention = "7d"

[provisioning]
backend = "cloud-do"          # host | incus | libvirt | proxmox | cloud-<anything>; not dispatched on
isolation = "vm"              # none | container | vm; not dispatched on
tier = "0"                    # "0" | "1" | "1.5" | "2"; published in the listing
resources = { cpu = 1, mem_mb = 1024, disk_gb = 25 }   # counted against host capacity
env = ["DO_TOKEN", "DO_REGION"]   # optional: operator env vars forwarded to hooks, §2

[os]
supports = ["debian"]         # non-empty; each entry is nixos | debian

[[params]]                    # zero or more; published in the listing; ≤64
key = "ssh_pubkey"
label = "Your SSH public key"
type = "string"               # string | number | int | integer | bool | boolean | other
required = true

[[operation]]                 # zero or more; published minus `hook`; ≤64
name = "status"
label = "VPS status"
kind = "request"              # request | interactive
hook = "status"               # bare filename under ops/; no "/", no "..", non-empty
# params = [ ... ]            # optional, same shape as [[params]], ≤64
```

Validation the daemon applies at load: `service.id` non-empty; `backend` and `isolation` each
one of the values above; `tier` in the four values;
`os.supports` non-empty and every entry `nixos` or `debian`; `env` has at most **16** names, each `1..=64` chars of `[A-Z0-9_]`
and never starting with `LNRENT`; every `hook` is a bare filename whose canonical path stays
inside `ops/`; every `operation.kind` is `request` or `interactive`; **operation names are
unique** within a recipe; the five lifecycle hooks exist; params and operations within the
bounds above.

Not validated at load: the three `[pricing]` durations. A string outside the grammar of
`listing.md` §3 (or a non-positive number) is **not rejected**; the reference daemon logs a
warning and bills on a **30-day fallback** while still publishing the raw string in the listing
`price` tag and in `order.invoice.period`. A recipe MUST use the grammar; the daemon does not
yet make it fail closed.

## 2. Process contract (all hooks)

| aspect | rule |
|--|--|
| invocation | executed directly (absolute path), no shell |
| stdin | one JSON document, then EOF; shape per hook in §3 |
| stdout | MUST be a single JSON value; the daemon parses all of stdout |
| stderr | captured (and size-capped) but **discarded on success**, and included in the operator's failure message **only on a non-zero exit**; a timeout or an invalid-stdout failure discards it too. Do not rely on stderr for diagnostics the operator should see. |
| exit code | `0` = success; anything else = failure, and stdout is ignored |
| timeout | **120 s**; on timeout the whole process group is killed and the hook is a failure |
| output cap | **1 MiB** on each of stdout and stderr; exceeding either is a failure |
| process group | the hook is the leader of its own group; on failure, timeout or daemon shutdown the whole group is killed, so a hook MUST NOT rely on backgrounded children surviving it |
| environment | **cleared**, then exactly: `PATH`, `HOME`, `LANG`, `LC_ALL`, `TZ`, `TMPDIR` (each only if the daemon has it) plus the manifest's `env` names (each only if the daemon has it). Nothing else, ever. |
| secrets | arrive on stdin or via a declared `env` name, never argv |
| privilege | daemon privilege, unsandboxed; recipes are trusted code |

## 3. Per-hook stdin and stdout

Every stdin document is an object. Fields marked *may be `null`* are `null` when the daemon has
no value. Hooks MUST ignore unknown fields.

### 3.1 `provision`

stdin:

```json
{
  "subscription": { "id": "…", "buyer_pubkey": "<hex>", "recipe_id": "do-vps", "box_id": "<box>", "params": { … } },
  "instance":     { "id": "inst:<subscription id>", "subscription_id": "…", "box_id": "<box>", "kind": "do-vps" },
  "params":       { … },
  "host":         { "box_id": "<box>", "backend": "cloud-do", "isolation": "vm", "tier": "0",
                    "os": ["debian"], "resources": { "cpu": 1, "mem_mb": 1024, "disk_gb": 25 } }
}
```

`params` is the buyer's validated order params (may be `null` if the stored row is
unparseable, which the daemon treats as a bug on its side).

stdout:

```json
{ "payload": { … }, "handles": { … } }
```

- `payload` is **required and MUST NOT be `null`**. It is delivered verbatim to the buyer as
  `provision.ready.payload`. A missing or null payload is a provisioning failure.
- `handles` is optional. The daemon stores it and passes it back to every later hook for this
  instance (`instance.handles` and top-level `handles`). Absent means `{}`.
- Any other top-level key is ignored.

**Idempotency:** `provision` is retried with backoff and MAY be re-run after a daemon crash. A
second run for the same `subscription.id` MUST converge on the same resources, not create a
second set.

**Cleanup after a failed provision:** the daemon runs `destroy` before refunding. Its stdin is
the **provision stdin above**, plus the last `handles` any attempt returned, mirrored as
top-level `handles` and as `instance.handles`; if no attempt returned handles, neither key is
present. So a `destroy` hook MUST tolerate the provision-shaped document, absent handles, and
partially created resources.

### 3.2 `suspend`, `resume`, `destroy`

stdin (identical shape for all three):

```json
{
  "subscription": { "id": "…", "buyer_pubkey": "<hex>" },
  "instance":     { "id": "inst:…", "subscription_id": "…", "box_id": "<box>", "kind": "do-vps",
                    "state": "RUNNING", "handles": { … } },
  "handles":      { … }
}
```

`handles` and `instance.handles` are the object `provision` returned, or `null` when none was
stored or it fails to parse. When no instance row exists at all, `instance` and `handles` are
absent and stdin is just `{ "subscription": { "id", "buyer_pubkey" } }`.

stdout: one JSON value (required, §2), whose content the daemon does not read. By
convention `{ "ok": true, "state": "suspended" }`.

**Idempotency:** each of these is guarded by a compare-and-swap on the daemon side but MAY be
re-run after a crash. `suspend` on an already-stopped instance, `resume` on a running one and
`destroy` on a destroyed one MUST all succeed.

A `destroy` that fails is retried periodically by the daemon with the stdin shape:

```json
{ "subscription": { "id": "…" }, "instance": { "subscription_id": "…", "handles": { … } }, "handles": { … } }
```

Note this retry omits `buyer_pubkey` and the other instance fields.

### 3.3 `healthcheck`

Required to exist by manifest validation; **not invoked by the daemon today**. Reserved: exit
`0` if healthy.

### 3.4 `ops/<hook>` (management operations)

stdin:

```json
{
  "subscription": { "id": "…", "buyer_pubkey": "<hex>", "state": "ACTIVE" },
  "instance":     { "id": "inst:…", "subscription_id": "…", "box_id": "<box>", "kind": "do-vps",
                    "state": "RUNNING", "handles": { … } },
  "op":           "restart",
  "params":       { … },
  "host":         { "backend": "cloud-do", "isolation": "vm", "tier": "0", "os": ["debian"],
                    "resources": { … } },
  "now":          1725000000
}
```

`instance` is `null` before provisioning. `params` is the buyer's `op.request.params` **after
the daemon validated it against the operation's declared `params`**: it is an object, every
`required` key is present, each declared key has its declared type (same rules as order
params), and **no undeclared key is present**. A hook therefore never sees a key it did not
declare; a request that fails any of these is answered `invalid_params` and the hook is not
run.

stdout: **MUST be a JSON object**; it is returned to the buyer verbatim as `op.result.data`. A
non-object, non-JSON stdout, non-zero exit or timeout becomes `op.result { status: "error" }`
with `hook_failed` or `timeout`.

**Not assumed idempotent.** The daemon guarantees a duplicate `op.request` never re-runs the
hook, so a hook like `restart` may have side effects freely.

### 3.5 `preflight` (optional)

Run by the operator's `lnrent preflight` command and before publication, never per order.
stdin is `{}`. Exit `0` if the recipe's provisioning parameters (its `env` values, provider
credentials, region or size slugs) are usable; any non-zero exit blocks publication. stdout
MUST still be one JSON value (§2 applies to every hook; an empty stdout is a failure), but its
content is not read. `{"ok":true}` is the convention.

## 4. Security expectations on a recipe

- Never print secrets to stderr; stderr goes to logs.
- Treat everything under `params` as buyer-supplied and hostile: quote it, never interpolate
  it into a shell command.
- No LLM or remote model call in any hook. The daemon cannot verify this; it is a review
  requirement for a recipe to ship.
- `payload` and every `ops/` stdout reach a buyer that may be an AI agent; keep them structured
  data, not instructions.
