//! The buyer flows (SPEC.md §5, §7, ADR-0014, lnrent-7fp.13): one [`BuyerClient`] method per
//! buyer action — discover listings, place an order, await provisioning, run management ops, renew,
//! resend delivery, cancel. Each builds an `lnrent_wire::Msg`, gift-wraps it to the operator,
//! awaits the correlated reply, verifies provenance (`sender == operator`) + correlation
//! (`request_id` / `subscription_id`), and returns a typed wire result. The buyer NEVER pays — an
//! order returns the invoice for out-of-band settlement (SPEC.md §4.7).

use std::time::Duration;

use lnrent_wire::{
    gift_unwrap, gift_wrap, parse_listing, BillingInvoice, BillingNotice, DeliveryResendRequest,
    Event, Msg, NostrSigner, OpRequest, OpResult, OpStatus, OperationDecl, OrderInvoice,
    OrderRequest, ParsedListing, ProvisionReady, PublicKey, RenewRequest, SubCancel, WireError,
};
use serde_json::Value;

use crate::error::BuyerError;
use crate::relay::{Clock, Relay, RelayError};

fn transport(e: RelayError) -> BuyerError {
    BuyerError::Transport(e.0)
}

/// A small upper bound on how long a pinned request holds an acceptable reply while a preferred one
/// for the same id may still be in flight. [`crate::relay::GiftWrapStream::shorten_deadline`]
/// preserves a nearer request deadline, and a fresh id never enters this window.
///
/// It SHRINKS the replay window rather than closing it: a preferred reply that lands more than this
/// after the stored one still loses, exactly as it does today. Waiting the full request timeout
/// instead would delay every honest pinned refusal by that timeout, which is the trade the bead
/// declined.
const PINNED_GRACE: Duration = Duration::from_secs(5);

/// How long a pinned request holds an `Acceptable` reply while waiting for a `Preferred` one.
///
/// The two carriers need different answers and the asymmetry is the point:
///
/// - `renew` uses [`PinnedGrace::Window`]. An honest refusal there is COMMON (a RESUMING notice,
///   a foreign-recipe error), so waiting the full deadline on every one would be the latency cost
///   the bead explicitly declined.
/// - `invoke_op` uses [`PinnedGrace::FullDeadline`]. A row-free refusal there is rare, while the
///   live reply can legitimately be slow: hooks get 120s (`daemon/src/runner.rs:25`) and the
///   shipped `restart` makes two curl calls at `--max-time 30` each (`recipes/do-vps/ops/restart`).
///   A 5s window would expire mid-hook and return the stale refusal — the buyer then retries under
///   a fresh id and the operation runs TWICE.
///
/// **This NARROWS the op double-execution hole; it does not close it, and the numbers above say
/// so.** The buyer's own exchange deadline defaults to 30s (`clients/cli/src/main.rs:41`), a
/// quarter of the hook budget. A hook that outlasts THAT ends the stream, and the held stale
/// refusal is returned anyway — so an agent told "unauthorized" can still retry under a fresh id
/// while the first hook is running. The residual is `lnrent-x1u`. The alternative — returning
/// `Timeout` whenever a held refusal survives to the deadline, which steers the agent to re-send
/// the SAME pinned id — trades this for a lone honest refusal surfacing as repeated timeouts
/// forever, so it is a real design choice and not an oversight to patch in passing.
#[derive(Debug, Clone, Copy)]
enum PinnedGrace {
    /// Hold for at most this long, then answer with what we have.
    Window(Duration),
    /// Hold until the request deadline the caller already chose.
    FullDeadline,
}

/// The `op.result` error codes the daemon answers WITHOUT claiming the `(sender, request_id)`
/// idempotency key. `daemon/src/op_dispatch.rs:249-254` states the contract: "Every AUTHORIZED
/// business outcome (unknown/invalid/hook failure) is a committed, cached `op.result` ... The four
/// AUTH rejects (unknown sub / not-owner / foreign-recipe / not-ACTIVE) reply an error but persist
/// NOTHING." Those four carry three codes — `unauthorized` answers both unknown-sub and not-owner so
/// neither leaks the other's existence (`op_dispatch.rs:1084`, `:1116`, `:1123`). Two daemon tests
/// pin it: `auth_rejects_are_row_free` (`:2045`) asserts they leave `op_invocation` empty, and
/// `foreign_recipe_refusal_leaves_the_request_id_usable_after_a_repoint` (`:1605`) re-sends the SAME
/// id after a repoint and shows the hook DOES then run — the daemon half of this very race.
///
/// That contract, not any transport marker, is the whole preferred/acceptable distinction for
/// `op.result` (lnrent-1jm): a re-sent pinned id can only see a STORED reply DIVERGE from the live
/// one when the first attempt persisted nothing, because an attempt that did claim the key makes the
/// daemon resend its cached terminal — byte-identical to the stored copy, so neither outranks the
/// other. Drift is safe both ways: a refusal code added later and missing here just returns as
/// promptly as it does today. One that later becomes cached is held for the carrier's
/// [`PinnedGrace`] — the short window on `renew`, the full request deadline on `invoke_op`, which
/// is the only path that consults [`ROW_FREE_OP_REFUSALS`].
///
/// `op_dispatch.rs` answers a FOURTH row-free — `invalid_request_id` (`:263-266`) — deliberately
/// excluded here, because buyer-id validation is deterministic on the pinned id: stored and live
/// can never disagree, so holding for a "better" reply would only add latency to a verdict that
/// cannot change.
const ROW_FREE_OP_REFUSALS: [&str; 3] = ["unauthorized", "not_active", "unavailable"];

/// Whether this `op.result` is one of the [`ROW_FREE_OP_REFUSALS`] — i.e. a reply the daemon can
/// have produced for an EARLIER attempt on this same pinned id while still running the live one.
fn is_row_free_op_refusal(r: &OpResult) -> bool {
    r.status == OpStatus::Error
        && r.error
            .as_ref()
            .is_some_and(|e| ROW_FREE_OP_REFUSALS.contains(&e.code.as_str()))
}

/// How a decoded reply rates against the request awaiting one (lnrent-1jm).
#[derive(Clone, Copy)]
enum ReplyMatch {
    /// Not a reply to this request — skip it and keep reading.
    Ignore,
    /// A reply to this request that a better one for the SAME id could supersede.
    Acceptable,
    /// The reply this request was waiting for; nothing outranks it, so it ends the exchange.
    Preferred,
}

/// The outcome of [`BuyerClient::renew`] (lnrent-zs2). A renewal request against an
/// ACTIVE/SUSPENDED sub is answered by a request-correlated `billing.invoice` (`Invoice`); a
/// request that lands while the sub is transiently RESUMING is answered by a request-correlated
/// `billing.notice` asking the buyer to retry once the resume completes (`Retry`, lnrent-z4u). Both
/// carry this request's `request_id`, so the `renew` matcher accepts either and the RESUMING case
/// surfaces as operator feedback instead of a timeout — while a relay-replayed stale notice from an
/// earlier request (different id) is ignored. A REFUSAL is not a variant here: the operator declining
/// to serve the subscription at all (lnrent-dvb) rides a request-correlated `order.error` and comes
/// back as `Err(BuyerError::Remote)`, the same shape a refused order gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewReply {
    /// A payable renewal invoice for the requested subscription.
    Invoice(BillingInvoice),
    /// A transient "retry in a moment" notice (the sub is mid-resume); no invoice was issued.
    Retry(BillingNotice),
}

/// A buyer talking to ONE operator over a relay. Holds the injected seams + the operator pubkey and
/// the per-exchange timeout; the host constructs it once and calls one method per CLI verb.
pub struct BuyerClient<'a, R: Relay, S: NostrSigner, C: Clock> {
    relay: &'a R,
    signer: &'a S,
    clock: &'a C,
    operator: PublicKey,
    timeout: Duration,
}

impl<'a, R: Relay, S: NostrSigner, C: Clock> BuyerClient<'a, R, S, C> {
    pub fn new(
        relay: &'a R,
        signer: &'a S,
        clock: &'a C,
        operator: PublicKey,
        timeout: Duration,
    ) -> Self {
        Self {
            relay,
            signer,
            clock,
            operator,
            timeout,
        }
    }

    // -- discovery (NIP-99 30402, SPEC.md §5.4) -------------------------------------------------

    /// Discover the operator's listings: fetch their kind-30402 events and parse each via
    /// `parse_listing` (which verifies the signature before trusting any field). Unverifiable /
    /// unparseable / foreign-authored events are silently skipped — discovery is best-effort.
    pub async fn discover_listings(&self) -> Result<Vec<ParsedListing>, BuyerError> {
        let events = self
            .relay
            .fetch_listings(&self.operator, self.timeout)
            .await
            .map_err(transport)?;
        let mut out = Vec::new();
        for ev in &events {
            // Trust only listings actually authored by the queried operator AND that verify+parse
            // (parse_listing calls event.verify()); a tampered/unsigned 30402 is dropped here.
            if ev.pubkey == self.operator {
                if let Ok(parsed) = parse_listing(ev) {
                    out.push(parsed);
                }
            }
        }
        Ok(out)
    }

    /// Fetch one listing by its addressable coordinate `30402:<pubkey>:<d>`. `NotFound` if the
    /// operator publishes no such (verifiable) listing.
    pub async fn get_listing(&self, listing_id: &str) -> Result<ParsedListing, BuyerError> {
        self.discover_listings()
            .await?
            .into_iter()
            .find(|p| p.listing_id == listing_id)
            .ok_or_else(|| {
                BuyerError::NotFound(format!(
                    "no listing {listing_id} published by operator {}",
                    self.operator.to_hex()
                ))
            })
    }

    /// The operator's published management-operation declarations (§5.4, §7.4), de-duplicated by
    /// name across listings. Advisory for discovery — the operator's recipe is authoritative at
    /// dispatch. The buyer renders its `ops` interface from these.
    pub async fn list_ops(&self) -> Result<Vec<OperationDecl>, BuyerError> {
        let mut ops: Vec<OperationDecl> = Vec::new();
        for listing in self.discover_listings().await? {
            for op in listing.listing.operations {
                if !ops.iter().any(|o| o.name == op.name) {
                    ops.push(op);
                }
            }
        }
        Ok(ops)
    }

    // -- order placement + provisioning (SPEC.md §5.1, §6, §7.2) --------------------------------

    /// Place an order against `listing_id`: send `order.request` and await `order.invoice` (or a
    /// structured `order.error`) correlated by `request_id`. Returns the invoice for the buyer to
    /// settle OUT-OF-BAND — buyer-core never pays (SPEC.md §4.7).
    pub async fn create_order(
        &self,
        listing_id: &str,
        params: Value,
        refund_dest: Option<String>,
    ) -> Result<OrderInvoice, BuyerError> {
        let id = self.clock.new_request_id();
        let request = self
            .wrap(Msg::OrderRequest(OrderRequest {
                id: id.clone(),
                listing_id: listing_id.to_string(),
                params,
                refund_dest,
            }))
            .await?;
        let want_id = id.clone();
        let operator = self.operator;
        let (sender, reply) = self
            .exchange(Some(&request), move |sender, m| {
                if sender != &operator {
                    return false;
                }
                match m {
                    Msg::OrderInvoice(inv) => inv.request_id == want_id,
                    Msg::OrderError(err) => err.request_id == want_id,
                    _ => false,
                }
            })
            .await?;
        self.check_sender(&sender, "order reply")?;
        match reply {
            Msg::OrderInvoice(inv) => Ok(inv),
            Msg::OrderError(err) => Err(BuyerError::Remote(err.error)),
            _ => unreachable!("the matcher restricts to order.invoice / order.error"),
        }
    }

    /// Await the operator's `provision.ready` for `subscription_id` (the credentials delivered after
    /// settlement → capture → provisioning). Passive: it sends nothing, just listens. The order's
    /// `order_id` IS its `subscription_id` (SPEC.md §6), so `order wait <order-id>` passes that here.
    pub async fn wait_provision(
        &self,
        subscription_id: &str,
    ) -> Result<ProvisionReady, BuyerError> {
        self.await_provision(None, subscription_id).await
    }

    /// Ask the operator to re-send the latest `provision.ready` for `subscription_id`
    /// (`delivery.resend.request`, the dropped-DM resync of §5.1) and return it. Also backs
    /// `subs status`, since the re-delivered payload reflects the subscription's current delivered
    /// state (there is no separate subscription-status message in M1a).
    pub async fn resend_delivery(
        &self,
        subscription_id: &str,
    ) -> Result<ProvisionReady, BuyerError> {
        let request = self
            .wrap(Msg::DeliveryResendRequest(DeliveryResendRequest {
                subscription_id: subscription_id.to_string(),
            }))
            .await?;
        self.await_provision(Some(&request), subscription_id).await
    }

    // -- billing + management (SPEC.md §5.1, §6.2, §7.4) ----------------------------------------

    /// Request a renewal invoice on demand: send `renew.request` and await the operator's reply.
    /// Two replies are accepted (lnrent-zs2): the request-correlated `billing.invoice`
    /// ([`RenewReply::Invoice`], the normal case) and — for a sub the operator is transiently
    /// resuming — a request-correlated `billing.notice` ([`RenewReply::Retry`], lnrent-z4u). The
    /// daemon answers a renew during RESUMING with the notice INSTEAD of an invoice, echoing this
    /// request's `request_id`, so the notice is normally a real answer rather than a timeout, and a
    /// stale one from an earlier request — carrying a different id — cannot masquerade as this
    /// request's reply. A request-correlated `order.error` is the third
    /// (lnrent-dvb): the operator refusing to serve this subscription at all, surfaced as
    /// [`BuyerError::Remote`] exactly as `create_order` surfaces one — and it is decided FIRST, so a
    /// subscription the operator does not serve gets that error whatever state it is in. An
    /// otherwise-invalid renewal (unknown sub / non-owner / non-renewable state of a subscription the
    /// operator DOES serve) is dropped with no reply, surfacing here as a timeout.
    ///
    /// With a pinned request id, a matching notice or error may be a stored reply from an earlier
    /// attempt, so it is held briefly to let this attempt's invoice take precedence (lnrent-1jm).
    pub async fn renew(&self, subscription_id: &str) -> Result<RenewReply, BuyerError> {
        let id = self.clock.new_request_id();
        let request = self
            .wrap(Msg::RenewRequest(RenewRequest {
                id: id.clone(),
                subscription_id: subscription_id.to_string(),
            }))
            .await?;
        let want_id = id.clone();
        let want_sub = subscription_id.to_string();
        let operator = self.operator;
        let (sender, reply) = self
            .exchange_preferred(Some(&request), PinnedGrace::Window(PINNED_GRACE), move |sender, m| {
                if sender != &operator {
                    return ReplyMatch::Ignore;
                }
                // The notice and error paths are not cached (`daemon/src/order_intake.rs:471-481,
                // 531-543`), so a pinned id can replay either before this attempt's invoice.
                match m {
                    Msg::BillingInvoice(bi)
                        if bi.request_id.as_deref() == Some(&want_id)
                            && bi.subscription_id == want_sub =>
                    {
                        ReplyMatch::Preferred
                    }
                    Msg::BillingNotice(n)
                        if n.request_id.as_deref() == Some(&want_id)
                            && n.subscription_id == want_sub
                            && n.state == "RESUMING" =>
                    {
                        ReplyMatch::Acceptable
                    }
                    // lnrent-dvb: the operator REFUSING this renew — e.g. the subscription belongs
                    // to a recipe this daemon does not serve, so it will not quote its own price for
                    // it. `order.error` is the wire's only error-bearing operator->buyer message
                    // outside `op.result` (`wire/src/dm.rs`), so it carries this too; without this
                    // arm the daemon's honest answer would be dropped here and the buyer would see
                    // an indistinguishable timeout. Correlated by `request_id` alone because
                    // `order.error` has no `subscription_id` field — the same correlation strength
                    // `create_order` accepts. Exact for a FRESH id (the default): an earlier
                    // request's replayed error carries a different id and cannot match. For a pinned
                    // id, `exchange_preferred` briefly holds this error or a RESUMING notice so a
                    // live invoice can take precedence without burning the full request timeout.
                    Msg::OrderError(e) if e.request_id == want_id => ReplyMatch::Acceptable,
                    _ => ReplyMatch::Ignore,
                }
            })
            .await?;
        self.check_sender(&sender, "renew reply")?;
        match reply {
            Msg::BillingInvoice(invoice) => Ok(RenewReply::Invoice(invoice)),
            Msg::BillingNotice(notice) => Ok(RenewReply::Retry(notice)),
            // Surfaced with the operator's own { code, message, retryable } (exit 6), unchanged —
            // identical handling to `create_order`, so an agent branches on renewal refusals exactly
            // as it does on order refusals.
            Msg::OrderError(err) => Err(BuyerError::Remote(err.error)),
            _ => unreachable!(
                "the matcher restricts to a request-correlated billing.invoice, RESUMING \
                 billing.notice, or order.error"
            ),
        }
    }

    /// Invoke a management operation: send `op.request` and await the `op.result` correlated by
    /// `request_id`. An `interactive`-kind op is rejected up-front with `unsupported_interactive`
    /// (Iroh sessions are out of scope for M1a, §9.2) without sending anything. An `op.result`
    /// error becomes a `Remote` error (exit 6); an `ok` result is returned for the caller to render.
    ///
    /// With a pinned request id, a matching [`ROW_FREE_OP_REFUSALS`] error may be a stored reply from
    /// an earlier attempt that persisted nothing, so it is held for the caller's FULL request
    /// deadline ([`PinnedGrace::FullDeadline`], `--timeout`, 30s by default — NOT the short
    /// [`PINNED_GRACE`] window `renew` uses) to let THIS
    /// attempt's result take precedence (lnrent-1jm). That matters more here than on `renew`: report
    /// a stale refusal for an attempt whose hook actually ran and the buyer retries under a fresh id,
    /// which runs a non-idempotent hook a SECOND time.
    pub async fn invoke_op(
        &self,
        subscription_id: &str,
        op: &str,
        op_kind: Option<&str>,
        params: Value,
    ) -> Result<OpResult, BuyerError> {
        // Refuse interactive BEFORE sending: per the listing's published declaration, an
        // interactive op rides an Iroh session this client does not implement.
        if let Some(kind) = op_kind {
            if kind != "request" {
                return Err(BuyerError::UnsupportedInteractive(format!(
                    "operation `{op}` is kind `{kind}`; only `request` ops are supported in M1a"
                )));
            }
        }
        let id = self.clock.new_request_id();
        let request = self
            .wrap(Msg::OpRequest(OpRequest {
                id: id.clone(),
                subscription_id: subscription_id.to_string(),
                op: op.to_string(),
                params,
            }))
            .await?;
        let want_id = id.clone();
        let want_sub = subscription_id.to_string();
        let want_op = op.to_string();
        let operator = self.operator;
        let (sender, reply) = self
            .exchange_preferred(Some(&request), PinnedGrace::FullDeadline, move |sender, m| {
                if sender != &operator {
                    return ReplyMatch::Ignore;
                }
                match m {
                    Msg::OpResult(r)
                        if r.request_id == want_id
                            && r.subscription_id == want_sub
                            && r.op == want_op =>
                    {
                        // The freshness discriminator is the DAEMON's cache contract, not anything in
                        // the transport: only a row-free refusal can be a stale copy the live attempt
                        // will disagree with. Everything else — an `ok`, or a committed error like
                        // `hook_failed` / `invalid_params` — is either this attempt's answer or the
                        // byte-identical cached resend of it, so it ends the exchange at once.
                        if is_row_free_op_refusal(r) {
                            ReplyMatch::Acceptable
                        } else {
                            ReplyMatch::Preferred
                        }
                    }
                    _ => ReplyMatch::Ignore,
                }
            })
            .await?;
        self.check_sender(&sender, "op.result")?;
        match reply {
            Msg::OpResult(result) => match result.status {
                OpStatus::Ok => Ok(result),
                OpStatus::Error => Err(BuyerError::Remote(result.error.unwrap_or_else(|| {
                    // op.result decode enforces error-present on status=error, so this is defensive.
                    WireError {
                        code: "error".into(),
                        message: "op.result error without an error body".into(),
                        retryable: false,
                    }
                }))),
            },
            _ => unreachable!("the matcher restricts to a request-correlated op.result"),
        }
    }

    /// Send `sub.cancel` for `subscription_id` and return once it is published. Naturally idempotent
    /// and fire-and-forget: the operator confirms asynchronously with an unsolicited
    /// `billing.notice`.
    pub async fn cancel(&self, subscription_id: &str) -> Result<(), BuyerError> {
        let request = self
            .wrap(Msg::SubCancel(SubCancel {
                subscription_id: subscription_id.to_string(),
            }))
            .await?;
        self.relay.publish(&request).await.map_err(transport)?;
        Ok(())
    }

    // -- internals ------------------------------------------------------------------------------

    /// Gift-wrap a message to the operator (NIP-17, SPEC.md §5.1).
    async fn wrap(&self, msg: Msg) -> Result<Event, BuyerError> {
        gift_wrap(self.signer, &self.operator, &msg)
            .await
            .map_err(|e| BuyerError::Internal(format!("gift-wrap: {e}")))
    }

    /// Subscribe to the buyer's gift wraps, optionally publish `request`, then return the first
    /// unwrapped message for which `want` holds (paired with its authenticated sender), or a
    /// timeout. Undecodable / unrelated gift wraps are skipped; callers include provenance +
    /// correlation in `want` so stale or planted replies cannot abort an exchange.
    async fn exchange<F>(
        &self,
        request: Option<&Event>,
        mut want: F,
    ) -> Result<(PublicKey, Msg), BuyerError>
    where
        F: FnMut(&PublicKey, &Msg) -> bool,
    {
        // This shim never yields `Acceptable`, so the grace value is inert here — every match is
        // `Preferred` and returns at once, pinned or not.
        self.exchange_preferred(request, PinnedGrace::Window(PINNED_GRACE), move |sender, msg| {
            if want(sender, msg) {
                ReplyMatch::Preferred
            } else {
                ReplyMatch::Ignore
            }
        })
        .await
    }

    /// As [`Self::exchange`], but a pinned id holds an `Acceptable` reply for `grace` so a
    /// `Preferred` reply correlated to the SAME request id can win — a short window on `renew`, the
    /// caller's full request deadline on `invoke_op`. Fresh ids return either match at once.
    async fn exchange_preferred<F>(
        &self,
        request: Option<&Event>,
        grace: PinnedGrace,
        mut want: F,
    ) -> Result<(PublicKey, Msg), BuyerError>
    where
        F: FnMut(&PublicKey, &Msg) -> ReplyMatch,
    {
        let me = self
            .signer
            .get_public_key()
            .await
            .map_err(|e| BuyerError::Internal(format!("signer pubkey: {e}")))?;
        let mut stream = self
            .relay
            .subscribe_giftwraps(&me, self.timeout)
            .await
            .map_err(transport)?;
        if let Some(event) = request {
            self.relay.publish(event).await.map_err(transport)?;
        }
        let pinned = self.clock.request_id_is_pinned();
        let mut held: Option<(PublicKey, Msg)> = None;
        loop {
            let next = match stream.next().await {
                Ok(next) => next,
                // A transport error AFTER we are already holding a real reply must not downgrade
                // the caller to `Transport`: the held reply is a genuine operator answer and is
                // strictly more useful than an error.
                //
                // Defensive: unreachable in every SHIPPED configuration today, and the reason has
                // two halves. `held` is only ever `Some` when pinned, and the only pinning `Clock`
                // is the CLI's `SysClock`, whose `NostrStream::next` never returns `Err`
                // (`clients/cli/src/relay.rs`). `BrowserGiftWrapStream` DOES propagate a read error
                // (`clients/web/src/relay.rs`), but `BrowserClock` cannot pin — it takes the
                // `request_id_is_pinned` default of `false` — so the composed system never reaches
                // here either. It is kept so a future pinning client cannot silently regress; note
                // that nothing pins it, since `FakeStream` has no error seam.
                Err(e) => return held.ok_or_else(|| transport(e)),
            };
            let Some(event) = next else { break };
            // A gift wrap that won't unwrap (not for us / undecodable) is skipped, not fatal.
            let Ok(unwrapped) = gift_unwrap(self.signer, &event).await else {
                continue;
            };
            match want(&unwrapped.sender, &unwrapped.msg) {
                ReplyMatch::Ignore => continue,
                ReplyMatch::Preferred => return Ok((unwrapped.sender, unwrapped.msg)),
                ReplyMatch::Acceptable if !pinned => return Ok((unwrapped.sender, unwrapped.msg)),
                ReplyMatch::Acceptable => {
                    // Enter the window once. `FullDeadline` deliberately does NOT shorten: the
                    // caller has already budgeted that wait, and truncating it is what lets a
                    // stale refusal outrun a slow-but-live reply.
                    if held.is_none() {
                        if let PinnedGrace::Window(within) = grace {
                            if !stream.shorten_deadline(within) {
                                return Ok((unwrapped.sender, unwrapped.msg));
                            }
                        }
                    }
                    held.get_or_insert((unwrapped.sender, unwrapped.msg));
                }
            }
        }
        held.ok_or_else(|| {
            BuyerError::Timeout("no correlated reply from the operator before the deadline".into())
        })
    }

    /// Shared `provision.ready` wait used by `wait_provision` (passive) and `resend_delivery`
    /// (after publishing the resend request): correlate by `subscription_id`, verify the sender.
    async fn await_provision(
        &self,
        request: Option<&Event>,
        subscription_id: &str,
    ) -> Result<ProvisionReady, BuyerError> {
        let sub = subscription_id.to_string();
        let operator = self.operator;
        let (sender, reply) = self
            .exchange(request, move |sender, m| {
                sender == &operator
                    && matches!(m, Msg::ProvisionReady(pr) if pr.subscription_id == sub)
            })
            .await?;
        self.check_sender(&sender, "provision.ready")?;
        match reply {
            Msg::ProvisionReady(pr) => Ok(pr),
            _ => unreachable!("the matcher restricts to a subscription-correlated provision.ready"),
        }
    }

    /// Reject a reply that did not come from the configured operator (provenance, exit 7).
    fn check_sender(&self, sender: &PublicKey, what: &str) -> Result<(), BuyerError> {
        if sender == &self.operator {
            Ok(())
        } else {
            Err(BuyerError::Protocol(format!(
                "{what} came from {} but the operator is {}",
                sender.to_hex(),
                self.operator.to_hex()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use lnrent_wire::{build_listing, Keys, Listing, OperationDecl, ParamDecl};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    use crate::relay::GiftWrapStream;

    const SCHEMA_VERSION: u32 = 1;

    /// A deterministic clock + counter-based request ids so tests can pre-build matching replies.
    #[derive(Default)]
    struct TestClock {
        n: AtomicU64,
        pinned: bool,
    }
    impl TestClock {
        fn pinned() -> Self {
            Self {
                n: AtomicU64::new(0),
                pinned: true,
            }
        }
    }
    impl Clock for TestClock {
        fn now_secs(&self) -> i64 {
            1_000
        }
        fn new_request_id(&self) -> String {
            format!("req-{}", self.n.fetch_add(1, Ordering::SeqCst))
        }
        fn request_id_is_pinned(&self) -> bool {
            self.pinned
        }
    }

    const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

    /// An in-memory relay: `listings` answers discovery; `replies` is drained into the gift-wrap
    /// stream when a flow subscribes (the STORED batch a Nostr REQ serves first); `late` holds
    /// replies that arrive after it, served only until the deadline is shortened; `published`
    /// records what the buyer sent. `can_shorten` models whether the stream supports shortening
    /// at all — false reproduces the browser stream, which takes the trait default.
    struct FakeRelay {
        listings: Vec<Event>,
        replies: Mutex<VecDeque<Event>>,
        late: Mutex<VecDeque<Event>>,
        published: Mutex<Vec<Event>>,
        can_shorten: Mutex<bool>,
    }
    impl FakeRelay {
        fn new() -> Self {
            Self {
                listings: Vec::new(),
                replies: Mutex::new(VecDeque::new()),
                late: Mutex::new(VecDeque::new()),
                published: Mutex::new(Vec::new()),
                can_shorten: Mutex::new(true),
            }
        }
        /// Model a stream taking the trait's DEFAULT `shorten_deadline` (the browser stream).
        fn without_deadline_shortening(self) -> Self {
            *self.can_shorten.lock().unwrap() = false;
            self
        }
        fn queue(&self, event: Event) {
            self.replies.lock().unwrap().push_back(event);
        }
        fn queue_late(&self, event: Event) {
            self.late.lock().unwrap().push_back(event);
        }
        fn published_len(&self) -> usize {
            self.published.lock().unwrap().len()
        }
    }
    #[async_trait]
    impl Relay for FakeRelay {
        async fn publish(&self, event: &Event) -> Result<(), RelayError> {
            self.published.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn fetch_listings(
            &self,
            _operator: &PublicKey,
            _timeout: Duration,
        ) -> Result<Vec<Event>, RelayError> {
            Ok(self.listings.clone())
        }
        async fn subscribe_giftwraps(
            &self,
            _recipient: &PublicKey,
            _timeout: Duration,
        ) -> Result<Box<dyn GiftWrapStream>, RelayError> {
            let events = std::mem::take(&mut *self.replies.lock().unwrap());
            let late = std::mem::take(&mut *self.late.lock().unwrap());
            Ok(Box::new(FakeStream {
                events,
                late,
                grace_entered: false,
                can_shorten: *self.can_shorten.lock().unwrap(),
            }))
        }
    }
    /// An in-memory relay stream. `events` are the STORED replies a Nostr REQ serves first;
    /// `late` stands in for replies that arrive after them — `next` serves `late` only until
    /// `shorten_deadline` is called, modelling "arrived after the shortened deadline".
    /// `can_shorten` is the seam for streams that take the trait's DEFAULT `shorten_deadline`
    /// (returning false), i.e. the browser stream.
    struct FakeStream {
        events: VecDeque<Event>,
        late: VecDeque<Event>,
        grace_entered: bool,
        can_shorten: bool,
    }
    #[async_trait]
    impl GiftWrapStream for FakeStream {
        async fn next(&mut self) -> Result<Option<Event>, RelayError> {
            if let Some(event) = self.events.pop_front() {
                return Ok(Some(event));
            }
            if self.grace_entered {
                Ok(None)
            } else {
                Ok(self.late.pop_front())
            }
        }
        fn shorten_deadline(&mut self, within: Duration) -> bool {
            assert!(
                within < REQUEST_TIMEOUT,
                "the grace window must sit well under the request timeout"
            );
            self.grace_entered = true;
            // `can_shorten` is false in the test that covers streams taking the trait's DEFAULT
            // impl (`clients/core/src/relay.rs`) — i.e. the browser stream, which does not
            // override it. Without this the `!shorten_deadline(..)` branch is unreachable.
            self.can_shorten
        }
    }

    /// Gift-wrap an operator → buyer reply (the same transport the real operator uses).
    async fn reply(from: &Keys, to: &PublicKey, msg: Msg) -> Event {
        gift_wrap(from, to, &msg).await.unwrap()
    }

    fn client<'a>(
        relay: &'a FakeRelay,
        signer: &'a Keys,
        clock: &'a TestClock,
        operator: PublicKey,
    ) -> BuyerClient<'a, FakeRelay, Keys, TestClock> {
        BuyerClient::new(relay, signer, clock, operator, REQUEST_TIMEOUT)
    }

    fn dummy_listing(operator: &PublicKey, ops: Vec<OperationDecl>) -> Listing {
        Listing {
            d: "dummy".into(),
            operator: operator.to_hex(),
            recipe_id: "dummy".into(),
            recipe_version: "0.1.0".into(),
            title: "Dummy".into(),
            summary: "test".into(),
            amount_sat: 100,
            period: "30d".into(),
            params: vec![ParamDecl {
                key: "region".into(),
                label: "Region".into(),
                ty: "string".into(),
                required: false,
            }],
            operations: ops,
            tier: None,
            version: SCHEMA_VERSION,
        }
    }

    fn signed_listing(op: &Keys, ops: Vec<OperationDecl>) -> Event {
        build_listing(&dummy_listing(&op.public_key(), ops))
            .unwrap()
            .sign_with_keys(op)
            .unwrap()
    }

    fn invoice(request_id: &str) -> Msg {
        Msg::OrderInvoice(OrderInvoice {
            request_id: request_id.into(),
            order_id: "ord:buyer:req-0".into(),
            bolt11: "lnbcmock1".into(),
            amount_sat: 100,
            period: "30d".into(),
            expires_at: 5_000,
        })
    }

    // order.request -> order.invoice, correlated by request_id (the happy path).
    #[tokio::test]
    async fn order_invoice_correlates_by_request_id() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), invoice("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let got = c
            .create_order(
                "30402:op:dummy",
                json!({}),
                Some("refunds@example.com".into()),
            )
            .await
            .expect("order.invoice");

        assert_eq!(got.request_id, "req-0");
        assert_eq!(got.bolt11, "lnbcmock1");
        assert_eq!(
            relay.published_len(),
            1,
            "exactly one gift-wrapped order.request was published"
        );
    }

    // A reply from someone other than the operator is skipped, not treated as the exchange result.
    #[tokio::test]
    async fn order_reply_from_wrong_sender_is_skipped() {
        let op = Keys::generate();
        let impostor = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        // Correct request_id, but sealed by an impostor: ignore it and keep reading.
        relay.queue(reply(&impostor, &buyer.public_key(), invoice("req-0")).await);
        relay.queue(reply(&op, &buyer.public_key(), invoice("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let got = c
            .create_order(
                "30402:op:dummy",
                json!({}),
                Some("refunds@example.com".into()),
            )
            .await
            .expect("operator-correlated invoice wins");

        assert_eq!(got.request_id, "req-0");
        assert_eq!(got.bolt11, "lnbcmock1");
    }

    // A reply from the operator whose request_id does not correlate is skipped; stale replay must
    // not poison the next order.create.
    #[tokio::test]
    async fn order_reply_with_wrong_request_id_is_skipped() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), invoice("not-mine")).await);
        relay.queue(reply(&op, &buyer.public_key(), invoice("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let got = c
            .create_order(
                "30402:op:dummy",
                json!({}),
                Some("refunds@example.com".into()),
            )
            .await
            .expect("operator-correlated invoice wins");

        assert_eq!(got.request_id, "req-0");
        assert_eq!(got.bolt11, "lnbcmock1");
    }

    // order.error from the operator surfaces as a Remote error (exit 6) with the operator's code.
    #[tokio::test]
    async fn order_error_is_remote_error() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        let err_msg = Msg::OrderError(lnrent_wire::OrderError {
            request_id: "req-0".into(),
            order_id: None,
            error: WireError {
                code: "capacity_full".into(),
                message: "no capacity".into(),
                retryable: true,
            },
        });
        relay.queue(reply(&op, &buyer.public_key(), err_msg).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .create_order("30402:op:dummy", json!({}), None)
            .await
            .unwrap_err();

        assert_eq!(err.exit_code(), 6);
        let env = err.envelope();
        assert_eq!(env.code, "capacity_full");
        assert!(env.retryable);
    }

    // Listing parse + provenance: a tampered 30402 is rejected (dropped from discovery); the
    // untampered one parses.
    #[tokio::test]
    async fn tampered_listing_is_rejected() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();

        let good = signed_listing(&op, vec![]);
        // Tamper the content AFTER signing: the event id/sig no longer match, so verify() fails.
        let mut value = serde_json::to_value(&good).unwrap();
        value["content"] =
            json!("{\"lnrent\":{\"version\":1,\"recipe\":{\"id\":\"evil\",\"version\":\"9\"}}}");
        let tampered: Event = serde_json::from_value(value).unwrap();

        let mut relay = FakeRelay::new();
        relay.listings = vec![tampered];
        let c = client(&relay, &buyer, &clock, op.public_key());
        assert!(
            c.discover_listings().await.unwrap().is_empty(),
            "a tampered/unsigned 30402 is not trusted"
        );

        relay.listings = vec![good];
        let c = client(&relay, &buyer, &clock, op.public_key());
        let listings = c.discover_listings().await.unwrap();
        assert_eq!(listings.len(), 1, "the untampered listing parses");
        assert_eq!(listings[0].listing.recipe_id, "dummy");
    }

    // op.request -> op.result round trip (correlated by request_id), returning the hook output.
    #[tokio::test]
    async fn op_request_round_trips() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        let result = Msg::OpResult(OpResult::ok(
            "req-0",
            "sub-1",
            "status",
            json!({"state": "running", "uptime_s": 42}),
        ));
        relay.queue(reply(&op, &buyer.public_key(), result).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let got = c
            .invoke_op("sub-1", "status", Some("request"), json!({}))
            .await
            .expect("op.result ok");

        assert_eq!(got.status, OpStatus::Ok);
        assert_eq!(got.data.unwrap()["state"], "running");
    }

    // An interactive op is rejected up-front (exit 3) and nothing is sent over the wire.
    #[tokio::test]
    async fn interactive_op_is_unsupported() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .invoke_op("sub-1", "shell", Some("interactive"), json!({}))
            .await
            .unwrap_err();

        assert_eq!(err.exit_code(), 3);
        assert_eq!(err.envelope().code, "unsupported_interactive");
        assert_eq!(
            relay.published_len(),
            0,
            "an interactive op is refused before any op.request is published"
        );
    }

    // op.result error (e.g. an unauthorized op) surfaces as a Remote error (exit 6).
    #[tokio::test]
    async fn op_error_is_remote_error() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        let result = Msg::OpResult(OpResult::err(
            "req-0",
            "sub-1",
            "status",
            WireError {
                code: "unauthorized".into(),
                message: "not your subscription".into(),
                retryable: false,
            },
        ));
        relay.queue(reply(&op, &buyer.public_key(), result).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .invoke_op("sub-1", "status", Some("request"), json!({}))
            .await
            .unwrap_err();

        assert_eq!(err.exit_code(), 6);
        assert_eq!(err.envelope().code, "unauthorized");
    }

    // A billing.invoice reply for the CANONICAL sub/request-id: renew returns RenewReply::Invoice
    // (rendered exactly as before the RenewReply split).
    fn billing_invoice(subscription_id: &str, request_id: Option<&str>) -> Msg {
        Msg::BillingInvoice(BillingInvoice {
            subscription_id: subscription_id.into(),
            request_id: request_id.map(Into::into),
            bolt11: "lnbcrenew1".into(),
            amount_sat: 100,
            due_at: 4_000,
            expires_at: 5_000,
        })
    }

    fn resuming_notice(subscription_id: &str, request_id: Option<&str>) -> Msg {
        Msg::BillingNotice(BillingNotice {
            subscription_id: subscription_id.into(),
            request_id: request_id.map(Into::into),
            state: "RESUMING".into(),
            message: "a renewal is being applied — please retry in a moment".into(),
        })
    }

    // renew.request -> billing.invoice, correlated by request_id + subscription_id (happy path).
    #[tokio::test]
    async fn renew_returns_invoice_on_billing_invoice() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("renew reply") {
            RenewReply::Invoice(inv) => {
                assert_eq!(inv.subscription_id, "sub-1");
                assert_eq!(inv.request_id.as_deref(), Some("req-0"));
                assert_eq!(inv.bolt11, "lnbcrenew1");
            }
            RenewReply::Retry(n) => panic!("expected an invoice, got a retry notice: {n:?}"),
        }
        assert_eq!(
            relay.published_len(),
            1,
            "exactly one gift-wrapped renew.request was published"
        );
    }

    // lnrent-zs2: a renew against a transiently RESUMING sub is answered by a request-correlated
    // billing.notice (echoing this request's id). It must surface as RenewReply::Retry — the buyer
    // sees the operator's feedback, NOT the old timeout.
    #[tokio::test]
    async fn renew_against_resuming_sub_returns_retry() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("renew reply (retry)") {
            RenewReply::Retry(notice) => {
                assert_eq!(notice.subscription_id, "sub-1");
                assert_eq!(notice.state, "RESUMING");
                assert!(
                    notice.message.contains("retry"),
                    "the notice carries the retry-in-a-moment message"
                );
            }
            RenewReply::Invoice(inv) => panic!("expected a retry notice, got an invoice: {inv:?}"),
        }
    }

    // Correlation is by subscription_id: a billing.notice for a DIFFERENT sub must not satisfy this
    // renew — keep reading until the correlated invoice for the requested sub arrives.
    #[tokio::test]
    async fn renew_skips_notice_for_a_different_sub() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("other-sub", Some("req-0"))).await);
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("correlated invoice wins") {
            RenewReply::Invoice(inv) => assert_eq!(inv.subscription_id, "sub-1"),
            RenewReply::Retry(n) => panic!("a foreign-sub notice must not satisfy renew: {n:?}"),
        }
    }

    // lnrent-zs2 (reviewer P2): relays replay stored gift wraps, so a same-sub RESUMING notice from
    // an EARLIER renew (different request_id) can be replayed ahead of this request's reply. The
    // request_id correlation must ignore that stale notice — otherwise the buyer would be told to
    // "retry in a moment" for a sub the operator is now answering with a real invoice (or dropping).
    #[tokio::test]
    async fn renew_ignores_stale_request_id_notice_and_prefers_correlated_invoice() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        // A replayed RESUMING notice from a PRIOR request (req-OLD), then THIS request's invoice.
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-OLD"))).await);
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("correlated invoice wins") {
            RenewReply::Invoice(inv) => {
                assert_eq!(inv.subscription_id, "sub-1");
                assert_eq!(inv.request_id.as_deref(), Some("req-0"));
            }
            RenewReply::Retry(n) => {
                panic!("a stale-request-id notice must not preempt the live invoice: {n:?}")
            }
        }
    }

    // lnrent-zs2 regression: billing.notice is a general type, and the buyer's giftwrap subscription
    // replays the full history. A stale same-sub notice for a NON-RESUMING state (e.g. CANCELLED)
    // must NOT be surfaced as a retry when the operator drops the renew (non-renewable state) with no
    // reply — that would tell the buyer to "retry in a moment" for a permanently non-renewable sub.
    // Only a RESUMING notice qualifies; anything else leaves the honest timeout intact.
    #[tokio::test]
    async fn renew_ignores_stale_non_resuming_notice_and_times_out() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        let cancelled = Msg::BillingNotice(BillingNotice {
            subscription_id: "sub-1".into(),
            request_id: None,
            state: "CANCELLED".into(),
            message: "subscription cancelled; service runs until the paid period ends".into(),
        });
        relay.queue(reply(&op, &buyer.public_key(), cancelled).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .renew("sub-1")
            .await
            .expect_err("a stale CANCELLED notice must not become a retry");
        assert!(
            matches!(err, BuyerError::Timeout(_)),
            "expected a timeout, got {err:?}"
        );
    }

    // lnrent-zs2 (reviewer P2, no-invoice variant): the buyer renews a sub the operator now DROPS as
    // non-renewable (no reply). Only a stale RESUMING notice from an EARLIER request (req-OLD) is
    // replayed. request_id correlation must ignore it and surface the honest timeout — NOT a false
    // "retry in a moment" for a permanently dead sub.
    #[tokio::test]
    async fn renew_ignores_stale_resuming_notice_from_another_request_and_times_out() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-OLD"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .renew("sub-1")
            .await
            .expect_err("a stale-request-id RESUMING notice must not become a retry");
        assert!(
            matches!(err, BuyerError::Timeout(_)),
            "expected a timeout, got {err:?}"
        );
    }

    // lnrent-dvb: the operator refuses to serve this subscription (it belongs to a recipe this
    // daemon does not serve) and answers with a request-correlated `order.error`. Without the
    // matcher arm this reply is discarded and the buyer sees a timeout — i.e. the daemon's honest
    // answer would be indistinguishable from an offline operator. It must surface as the operator's
    // OWN structured error (exit 6), not a timeout (exit 4).
    #[tokio::test]
    async fn renew_refusal_surfaces_the_operators_order_error() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        let refusal = Msg::OrderError(lnrent_wire::OrderError {
            request_id: "req-0".into(),
            order_id: None,
            error: WireError {
                code: "unavailable".into(),
                message: "this subscription is not currently served here".into(),
                retryable: true,
            },
        });
        relay.queue(reply(&op, &buyer.public_key(), refusal).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .renew("sub-1")
            .await
            .expect_err("a refusal is not a renewal");
        assert_eq!(err.exit_code(), 6, "remote error, NOT the timeout's 4");
        let env = err.envelope();
        assert_eq!(env.code, "unavailable");
        assert_eq!(
            env.message,
            "this subscription is not currently served here"
        );
        assert!(env.retryable);
    }

    // lnrent-dvb correlation: `order.error` carries no `subscription_id`, so `request_id` is the
    // whole correlation. A replayed refusal from an EARLIER request must not be mistaken for this
    // one's reply — otherwise a relay redelivery could turn a live renewal into a stale refusal.
    #[tokio::test]
    async fn renew_ignores_an_order_error_for_another_request() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        let stale = Msg::OrderError(lnrent_wire::OrderError {
            request_id: "req-OLD".into(),
            order_id: None,
            error: WireError {
                code: "unavailable".into(),
                message: "this subscription is not currently served here".into(),
                retryable: true,
            },
        });
        relay.queue(reply(&op, &buyer.public_key(), stale).await);
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("correlated invoice wins") {
            RenewReply::Invoice(inv) => assert_eq!(inv.request_id.as_deref(), Some("req-0")),
            RenewReply::Retry(n) => panic!("expected the correlated invoice, got {n:?}"),
        }
    }

    fn refusal(request_id: &str) -> Msg {
        Msg::OrderError(lnrent_wire::OrderError {
            request_id: request_id.into(),
            order_id: None,
            error: WireError {
                code: "unavailable".into(),
                message: "this subscription is not currently served here".into(),
                retryable: true,
            },
        })
    }

    async fn fake_renew(
        pinned: bool,
        replies: Vec<Msg>,
        late: Option<Msg>,
    ) -> Result<RenewReply, BuyerError> {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = if pinned {
            TestClock::pinned()
        } else {
            TestClock::default()
        };
        let relay = FakeRelay::new();
        for msg in replies {
            relay.queue(reply(&op, &buyer.public_key(), msg).await);
        }
        if let Some(msg) = late {
            relay.queue_late(reply(&op, &buyer.public_key(), msg).await);
        }
        client(&relay, &buyer, &clock, op.public_key())
            .renew("sub-1")
            .await
    }

    #[tokio::test]
    async fn pinned_renew_prefers_a_live_invoice_over_a_stored_resuming_notice() {
        let got = fake_renew(
            true,
            vec![
                resuming_notice("sub-1", Some("req-0")),
                billing_invoice("sub-1", Some("req-0")),
            ],
            None,
        )
        .await;
        let RenewReply::Invoice(inv) = got.expect("renew reply") else {
            panic!("stored notice preempted the live invoice")
        };
        assert_eq!(inv.request_id.as_deref(), Some("req-0"));
        assert_eq!(inv.bolt11, "lnbcrenew1");
    }

    #[tokio::test]
    async fn pinned_renew_prefers_a_live_invoice_over_a_stored_order_error() {
        let got = fake_renew(
            true,
            vec![refusal("req-0"), billing_invoice("sub-1", Some("req-0"))],
            None,
        )
        .await;
        let RenewReply::Invoice(inv) = got.expect("renew reply") else {
            panic!("stored error preempted the live invoice")
        };
        assert_eq!(inv.request_id.as_deref(), Some("req-0"));
    }

    // The far-off invoice makes this fail if the grace wait is unbounded: an honest refusal comes
    // back one PINNED_GRACE later, never at the full request deadline.
    #[tokio::test]
    async fn pinned_renew_returns_a_lone_resuming_notice_within_the_grace_window() {
        let got = fake_renew(
            true,
            vec![resuming_notice("sub-1", Some("req-0"))],
            Some(billing_invoice("sub-1", Some("req-0"))),
        )
        .await;
        let RenewReply::Retry(notice) = got.expect("notice is a real answer") else {
            panic!("grace window read the late invoice")
        };
        assert_eq!(notice.state, "RESUMING");
    }

    // The live invoice makes this fail if the grace window is applied unconditionally — a fresh id
    // has no stored reply to outrank, so it must answer with the first acceptable reply at once.
    #[tokio::test]
    async fn unpinned_renew_returns_its_first_acceptable_reply_with_no_grace_window() {
        let got = fake_renew(
            false,
            vec![
                resuming_notice("sub-1", Some("req-0")),
                billing_invoice("sub-1", Some("req-0")),
            ],
            None,
        )
        .await;
        let RenewReply::Retry(notice) = got.expect("first reply answers") else {
            panic!("fresh id entered the grace window")
        };
        assert_eq!(notice.state, "RESUMING");
    }

    fn op_error(code: &str) -> Msg {
        Msg::OpResult(OpResult::err(
            "req-0",
            "sub-1",
            "restart",
            WireError {
                code: code.into(),
                message: "refused".into(),
                retryable: true,
            },
        ))
    }

    fn op_ok() -> Msg {
        Msg::OpResult(OpResult::ok(
            "req-0",
            "sub-1",
            "restart",
            json!({"restarted": true}),
        ))
    }

    async fn fake_op(
        pinned: bool,
        replies: Vec<Msg>,
        late: Option<Msg>,
    ) -> Result<OpResult, BuyerError> {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = if pinned {
            TestClock::pinned()
        } else {
            TestClock::default()
        };
        let relay = FakeRelay::new();
        for msg in replies {
            relay.queue(reply(&op, &buyer.public_key(), msg).await);
        }
        if let Some(msg) = late {
            relay.queue_late(reply(&op, &buyer.public_key(), msg).await);
        }
        client(&relay, &buyer, &clock, op.public_key())
            .invoke_op("sub-1", "restart", Some("request"), json!({}))
            .await
    }

    // The P1 the reviewer panel caught: a FIXED grace window is wrong for `ops`. A management hook
    // gets 120s (`daemon/src/runner.rs:25`) and the shipped `restart` makes two curl calls at
    // `--max-time 30` each, so a live result can legitimately arrive well after any short window.
    // `late` is served only until the deadline is SHORTENED, so this test passes only because
    // `invoke_op` uses `PinnedGrace::FullDeadline` and never shortens. Under a fixed window it
    // returns the stored refusal, the buyer retries under a fresh id, and the droplet reboots twice.
    #[tokio::test]
    async fn pinned_op_waits_past_any_short_window_for_a_slow_live_result() {
        let got = fake_op(true, vec![op_error("unauthorized")], Some(op_ok()))
            .await
            .expect("the slow live result, not the stored refusal");
        assert_eq!(got.status, OpStatus::Ok, "a slow hook must still win");
    }

    // A stream that cannot shorten its deadline takes the trait's DEFAULT `shorten_deadline`
    // (`clients/core/src/relay.rs`) — that is the browser stream. `renew` must then answer with the
    // acceptable reply immediately rather than silently holding it for a window it cannot enforce.
    #[tokio::test]
    async fn pinned_renew_returns_at_once_when_the_stream_cannot_shorten() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new().without_deadline_shortening();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-0"))).await);
        relay.queue_late(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);
        match client(&relay, &buyer, &clock, op.public_key())
            .renew("sub-1")
            .await
            .expect("the acceptable reply, returned at once")
        {
            RenewReply::Retry(_) => {}
            RenewReply::Invoice(i) => {
                panic!("an unshortenable stream must not hold for a late invoice: {i:?}")
            }
        }
    }

    // The bead's worst carrier: a pinned id re-sent after a row-free refusal, where the daemon HAS
    // now run the hook. Reporting the stored refusal would have the buyer retry under a fresh id and
    // run a non-idempotent hook twice.
    #[tokio::test]
    async fn pinned_op_prefers_a_live_result_over_a_stored_row_free_refusal() {
        let got = fake_op(true, vec![op_error("unauthorized"), op_ok()], None)
            .await
            .expect("the live result, not the stored refusal");
        assert_eq!(got.status, OpStatus::Ok);
        assert_eq!(got.data.unwrap()["restarted"], true);
    }

    // A live hook FAILURE outranks a stored refusal too: `hook_failed` is a committed, cached
    // terminal (`daemon/src/op_dispatch.rs:249-254`), so it can only be this attempt's own answer —
    // and reporting it truthfully is what stops the buyer retrying a hook that already ran.
    #[tokio::test]
    async fn pinned_op_prefers_a_live_hook_failure_over_a_stored_row_free_refusal() {
        let err = fake_op(
            true,
            vec![op_error("not_active"), op_error("hook_failed")],
            None,
        )
        .await
        .expect_err("an op error is a Remote error");
        assert_eq!(err.envelope().code, "hook_failed");
        assert_eq!(err.exit_code(), 6);
    }

    // An honest refusal is still a refusal — never swallowed, never converted to a timeout.
    //
    // NOTE the deliberate asymmetry, and its cost: because `invoke_op` uses
    // `PinnedGrace::FullDeadline`, a LONE row-free refusal on a PINNED op now surfaces at the
    // request deadline rather than one short window after the reply. That is the accepted price of
    // closing the double-execution hole — a hook may legitimately outlast any fixed window, so
    // truncating the wait is what lets a stale refusal beat a live result. Row-free refusals on
    // `ops` are rare and the caller already budgeted `--timeout`; `renew`, where honest refusals
    // are COMMON, keeps the short window (see `PinnedGrace`). An earlier draft of this test
    // asserted the opposite and encoded the defect.
    #[tokio::test]
    async fn pinned_op_still_surfaces_a_lone_row_free_refusal() {
        let err = fake_op(true, vec![op_error("unavailable")], None)
            .await
            .expect_err("an honest refusal is not swallowed");
        assert_eq!(err.envelope().code, "unavailable");
        assert_eq!(err.exit_code(), 6);
    }

    // The live `ok` makes this fail if the grace window is applied unconditionally — a fresh id has
    // no stored reply to outrank, so its first correlated reply answers immediately.
    #[tokio::test]
    async fn unpinned_op_returns_its_first_reply_with_no_grace_window() {
        let err = fake_op(false, vec![op_error("unauthorized"), op_ok()], None)
            .await
            .expect_err("first reply answers");
        assert_eq!(err.envelope().code, "unauthorized");
    }
}
