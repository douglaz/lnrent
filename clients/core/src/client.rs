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

/// How long a PINNED-`--request-id` exchange keeps reading after an acceptable reply has already
/// matched, looking for the preferred one (lnrent-1jm). Small on purpose: it is pure added latency
/// on a path that already holds a correlated answer, and it must stay well under the CLI's
/// per-exchange deadline (`--timeout`, default 30s in `clients/cli/src/main.rs:40-42`). When the
/// caller sets a shorter deadline than this, the caller's wins —
/// [`crate::relay::GiftWrapStream::shorten_deadline`] never extends.
///
/// It costs the DEFAULT path nothing: a freshly minted request id has no stored reply to race, so
/// [`Clock::request_id_is_pinned`] is false and this window is never entered.
///
/// What being small costs in REACH, stated rather than left implied: the window has to cover a
/// whole operator round-trip, because the stored reply lands as the REQ replays — before this
/// attempt's request is even published (`clients/cli/src/relay.rs:67-79` subscribes, settles, and
/// only then does `exchange` publish). An answer slower than this closes the window first and the
/// held stale reply is returned, exactly as it was before lnrent-1jm; a long-running op hook is
/// the realistic case. That is a narrower fix, not a regression, and this const is the only lever
/// on it — widening buys reach at the price of latency every honest pinned refusal pays.
const PINNED_GRACE: Duration = Duration::from_secs(2);

/// How a decoded reply rates against the request awaiting one (lnrent-1jm) — the three-way answer
/// [`BuyerClient::exchange`]'s matcher gives.
///
/// The `Acceptable`/`Preferred` split exists for ONE hazard: a Nostr REQ returns a relay's STORED
/// matches before its live ones, and a re-sent PINNED request id can therefore surface the previous
/// attempt's reply — for the carriers the daemon does not cache, that stale reply outranks nothing
/// and must not end the exchange while this attempt's real answer is still in flight. On the
/// default (freshly minted, never-published) id the split is inert: `Acceptable` returns as promptly
/// as `Preferred`, because no stored reply can exist to prefer anything over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyMatch {
    /// Not a reply to this request — skip it and keep reading.
    No,
    /// A reply to this request that a better-correlated one for the SAME id could supersede.
    Acceptable,
    /// The reply this request was waiting for. Ends the exchange immediately; nothing outranks it.
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
                    return ReplyMatch::No;
                }
                // Both arms are PREFERRED — no lnrent-1jm grace window here even on a pinned id.
                // `order.request` is the one carrier whose answer the daemon DOES cache on
                // `(sender, request_id)`: the invoice commits to `inbound_request` inside the
                // order's own transaction (`daemon/src/order_intake.rs:296-299`), an error commits
                // through `cache_response_row` (`:770-773`), and a duplicate is answered from that
                // row before anything else runs (`:144-149`). The stored copy and the live answer
                // are therefore the same message — there is nothing to prefer.
                match m {
                    Msg::OrderInvoice(inv) if inv.request_id == want_id => ReplyMatch::Preferred,
                    Msg::OrderError(err) if err.request_id == want_id => ReplyMatch::Preferred,
                    _ => ReplyMatch::No,
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
    /// request's `request_id`, so the notice is accepted immediately (no invoice is coming) and a
    /// relay-replayed stale RESUMING notice from an earlier request — carrying a different id —
    /// cannot masquerade as this request's reply. A request-correlated `order.error` is the third
    /// (lnrent-dvb): the operator refusing to serve this subscription at all, surfaced as
    /// [`BuyerError::Remote`] exactly as `create_order` surfaces one — and it is decided FIRST, so a
    /// subscription the operator does not serve gets that error whatever state it is in. An
    /// otherwise-invalid renewal (unknown sub / non-owner / non-renewable state of a subscription the
    /// operator DOES serve) is dropped with no reply, surfacing here as a timeout.
    ///
    /// When the caller PINNED the request id (`--request-id`), the notice and the refusal are held
    /// briefly rather than returned on sight, so a copy of them left on the relay by an earlier
    /// attempt cannot beat this attempt's invoice — see the private `ReplyMatch` / `PINNED_GRACE`
    /// (lnrent-1jm). On the default fresh id nothing changes, in either answer or latency.
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
            .exchange(Some(&request), move |sender, m| {
                if sender != &operator {
                    return ReplyMatch::No;
                }
                // Why the two non-invoice answers below are ACCEPTABLE rather than preferred
                // (lnrent-1jm): neither is cached by the daemon — a renew leaves the
                // `(sender, request_id)` key unclaimed on both paths, the refusal arm saying so
                // outright (`daemon/src/order_intake.rs:471-481`) and the RESUMING arm replying
                // and returning without a durable write of any kind (`:531-543`) — so a re-sent
                // pinned id can find the previous attempt's notice or refusal STILL STORED on the
                // relay while this attempt's invoice is in flight. NIP-59 randomizes `created_at` by
                // up to days, so no `since` filter can exclude the stored copy. `exchange` holds
                // the stale answer for PINNED_GRACE and lets the invoice win if it arrives. On the
                // default fresh id there is no stored copy to race, so both come straight back with
                // no added latency, exactly as before.
                match m {
                    // The renewal invoice answering THIS request (request_id + sub correlated).
                    // PREFERRED: it is what a renew asked for, and the daemon mints it only for the
                    // live attempt — so it outranks either answer below whenever both are on offer.
                    Msg::BillingInvoice(bi)
                        if bi.request_id.as_deref() == Some(&want_id)
                            && bi.subscription_id == want_sub =>
                    {
                        ReplyMatch::Preferred
                    }
                    // The transient-RESUMING answer to THIS request (lnrent-z4u/zs2): the daemon
                    // replies with a request-correlated billing.notice INSTEAD of an invoice, so it
                    // is a real answer — no invoice is coming for this request. request_id
                    // correlation excludes a relay-replayed stale RESUMING notice from an earlier
                    // request (its id differs), and only state "RESUMING" qualifies (the operator
                    // emits same-sub notices for ACTIVE/SUSPENDED/CANCELLED too).
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
                    // `create_order` accepts.
                    Msg::OrderError(e) if e.request_id == want_id => ReplyMatch::Acceptable,
                    _ => ReplyMatch::No,
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
    /// When the caller PINNED the request id (`--request-id`), an error result is held briefly in
    /// case this attempt's own result follows — a re-sent pinned id whose first attempt was refused
    /// row-free really does run the hook, and reporting the stored refusal would push the buyer into
    /// a fresh-id retry that runs it twice (lnrent-1jm; see the matcher below for what that does and
    /// does not close). On the default fresh id nothing changes.
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
            .exchange(Some(&request), move |sender, m| {
                if sender != &operator {
                    return ReplyMatch::No;
                }
                match m {
                    Msg::OpResult(r)
                        if r.request_id == want_id
                            && r.subscription_id == want_sub
                            && r.op == want_op =>
                    {
                        // lnrent-1jm. BOTH the stale copy and the live answer are an `op.result`
                        // for the same (request_id, subscription_id, op) — the wire carries no
                        // attempt marker and `gift_unwrap` surfaces no timestamp
                        // (`wire/src/wrap.rs:36-39`), so the client CANNOT tell "stored" from
                        // "live". What it can rank is the answers themselves, and that is enough
                        // for the case that actually costs money: the daemon's row-free rejects
                        // (`unauthorized` / `unavailable` / `not_active`) persist nothing
                        // (`daemon/src/op_dispatch.rs:96-108`), so re-sending a pinned id after the
                        // operator fixes the condition really does run the hook. If that run
                        // SUCCEEDED, reporting the stored refusal instead would send the buyer back
                        // with a fresh id and execute a non-idempotent op twice. So an `ok` is
                        // preferred over an `error` for the same id.
                        //
                        // What this does NOT close, stated plainly: if the live run reached the
                        // hook and FAILED, its `op.result` error is indistinguishable from the
                        // stored refusal and the held one may be reported. That is bounded — the
                        // daemon caches a hook failure durably and resends it
                        // (`daemon/src/op_dispatch.rs:8-11,27-29`), so re-sending the SAME pinned
                        // id returns that cached terminal without re-running the hook.
                        match r.status {
                            OpStatus::Ok => ReplyMatch::Preferred,
                            OpStatus::Error => ReplyMatch::Acceptable,
                        }
                    }
                    _ => ReplyMatch::No,
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

    /// Subscribe to the buyer's gift wraps, optionally publish `request`, then return the unwrapped
    /// message `want` selects (paired with its authenticated sender), or a timeout. Undecodable /
    /// unrelated gift wraps are skipped; callers include provenance + correlation in `want` so stale
    /// or planted replies cannot abort an exchange.
    ///
    /// A [`ReplyMatch::Preferred`] reply always ends the exchange on the spot. A
    /// [`ReplyMatch::Acceptable`] one does too — UNLESS the request id was pinned by the caller
    /// ([`Clock::request_id_is_pinned`]), in which case it is held while the SAME subscription is
    /// read for a further [`PINNED_GRACE`], because it may be a previous attempt's reply replayed
    /// from relay storage rather than this attempt's (lnrent-1jm). If a preferred reply turns up in
    /// that window it wins; otherwise the held reply is returned, so an honest refusal is reported
    /// as a refusal — just [`PINNED_GRACE`] later, never at the full request deadline.
    ///
    /// The window reads the stream it is already on. Re-subscribing would be strictly worse: the
    /// host's notification receiver is created at subscribe time, so a reply already buffered
    /// against the first subscription would be invisible to a second one, and the relay's stored
    /// replay does NOT make it up — `nostr-relay-pool`'s message handler suppresses the
    /// notification for an event it has already saved: `send_notification` sits inside the
    /// `DatabaseEventStatus::NotExistent` arm ALONE, so the `Saved => {}` arm falls through to the
    /// return with no `Event` notification emitted
    /// (`nostr-relay-pool-0.44.1/src/relay/inner.rs:1216-1265`).
    async fn exchange<F>(
        &self,
        request: Option<&Event>,
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
                // A relay failure while looking for something BETTER must not destroy the
                // correlated reply already in hand: without the grace window that reply had already
                // been returned, so surfacing a transport error here would be a pinned-path
                // regression. With nothing held, this is the transport error it has always been.
                Err(e) => return held.ok_or_else(|| transport(e)),
            };
            let Some(event) = next else { break };
            // A gift wrap that won't unwrap (not for us / undecodable) is skipped, not fatal.
            let Ok(unwrapped) = gift_unwrap(self.signer, &event).await else {
                continue;
            };
            match want(&unwrapped.sender, &unwrapped.msg) {
                ReplyMatch::No => continue,
                ReplyMatch::Preferred => return Ok((unwrapped.sender, unwrapped.msg)),
                // Fresh id: nothing better can exist under an id that was never published, so this
                // IS the answer — returned with no added latency, exactly as before lnrent-1jm.
                ReplyMatch::Acceptable if !pinned => return Ok((unwrapped.sender, unwrapped.msg)),
                ReplyMatch::Acceptable => {
                    if held.is_none() {
                        // Bound the extra wait BEFORE taking it. A host that cannot shorten its
                        // deadline gets no grace window rather than an unbounded one: answer now,
                        // as before. The first acceptable reply is the one held — a later one is no
                        // more likely to be the live attempt's, and churning the answer would make
                        // the result depend on relay delivery order.
                        if !stream.shorten_deadline(PINNED_GRACE) {
                            return Ok((unwrapped.sender, unwrapped.msg));
                        }
                        held = Some((unwrapped.sender, unwrapped.msg));
                    }
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
                // PREFERRED: this wait is correlated by subscription_id, not by a request id, so a
                // pinned `--request-id` changes nothing here and there is no second reply to rank.
                if sender == &operator
                    && matches!(m, Msg::ProvisionReady(pr) if pr.subscription_id == sub)
                {
                    ReplyMatch::Preferred
                } else {
                    ReplyMatch::No
                }
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crate::relay::GiftWrapStream;

    const SCHEMA_VERSION: u32 = 1;

    /// A deterministic clock + counter-based request ids so a test can pre-build the matching reply.
    /// `pinned` mirrors the CLI's `--request-id`: the ids stay counter-derived either way, because
    /// what lnrent-1jm keys off is the caller having PINNED an id, not the id's shape.
    #[derive(Default)]
    struct TestClock {
        n: AtomicU64,
        pinned: bool,
    }
    impl TestClock {
        /// A clock reporting a caller-pinned request id (`--request-id`), the only mode in which
        /// buyer-core enters the grace window.
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

    /// An in-memory relay: `listings` answers discovery; `replies` is drained into the gift-wrap
    /// stream when a flow subscribes; `published` records what the buyer sent.
    ///
    /// `late` models the rest of the subscription's lifetime — replies that would still arrive
    /// before the request timeout, but only AFTER the lnrent-1jm grace window has closed. The
    /// stream withholds them once its deadline has been shortened, which is what makes "the grace
    /// window is bounded" a falsifiable assertion rather than a wall-clock guess: an unbounded
    /// window would read them and return the wrong reply.
    struct FakeRelay {
        listings: Vec<Event>,
        replies: Mutex<VecDeque<Event>>,
        late: Mutex<VecDeque<Event>>,
        published: Mutex<Vec<Event>>,
        /// How many times a stream from this relay had its deadline shortened — i.e. how many times
        /// the grace window was entered.
        shortened: Arc<AtomicUsize>,
    }
    impl FakeRelay {
        fn new() -> Self {
            Self {
                listings: Vec::new(),
                replies: Mutex::new(VecDeque::new()),
                late: Mutex::new(VecDeque::new()),
                published: Mutex::new(Vec::new()),
                shortened: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn queue(&self, event: Event) {
            self.replies.lock().unwrap().push_back(event);
        }
        /// Queue a reply that arrives only after the grace window would have closed.
        fn queue_late(&self, event: Event) {
            self.late.lock().unwrap().push_back(event);
        }
        fn published_len(&self) -> usize {
            self.published.lock().unwrap().len()
        }
        fn grace_windows_entered(&self) -> usize {
            self.shortened.load(Ordering::SeqCst)
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
                shortened: self.shortened.clone(),
            }))
        }
    }
    struct FakeStream {
        events: VecDeque<Event>,
        late: VecDeque<Event>,
        shortened: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl GiftWrapStream for FakeStream {
        async fn next(&mut self) -> Result<Option<Event>, RelayError> {
            if let Some(event) = self.events.pop_front() {
                return Ok(Some(event));
            }
            // The prompt replies are exhausted. A deadline that has been brought forward expires
            // here (`None`); an untouched one still has the rest of the request timeout to run, so
            // the late replies arrive.
            if self.shortened.load(Ordering::SeqCst) > 0 {
                return Ok(None);
            }
            Ok(self.late.pop_front())
        }
        fn shorten_deadline(&mut self, within: Duration) -> bool {
            assert!(
                within < Duration::from_secs(5),
                "the grace window must sit well under the 5s request timeout these tests use"
            );
            self.shortened.fetch_add(1, Ordering::SeqCst);
            true
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
        BuyerClient::new(relay, signer, clock, operator, Duration::from_secs(5))
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

    // -- lnrent-1jm: a re-sent PINNED `--request-id` can race a STORED reply -----------------------
    //
    // A Nostr REQ returns a relay's stored matches before its live ones, and NIP-59 randomizes a
    // gift wrap's `created_at` by up to days, so no `since` filter can exclude the stored copy. The
    // daemon caches neither the RESUMING notice, the renew refusal, nor a row-free op reject, so
    // re-sending a pinned id genuinely produces a second, live answer while the first is still on
    // the relay. These tests drive the real `exchange` path with events fed in stored-then-live
    // order.

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

    fn op_reject(request_id: &str) -> Msg {
        Msg::OpResult(OpResult::err(
            request_id,
            "sub-1",
            "restart",
            WireError {
                code: "not_active".into(),
                message: "subscription is not ACTIVE".into(),
                retryable: false,
            },
        ))
    }

    fn op_ok(request_id: &str) -> Msg {
        Msg::OpResult(OpResult::ok(
            request_id,
            "sub-1",
            "restart",
            json!({"restarted": true}),
        ))
    }

    // The headline case. Pinned id, the previous attempt's RESUMING notice is still stored on the
    // relay and lands first; the live invoice follows. The invoice must win — otherwise the buyer is
    // told to "retry in a moment" for a renewal the operator has already priced.
    #[tokio::test]
    async fn pinned_renew_prefers_a_live_invoice_over_a_stored_resuming_notice() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-0"))).await);
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("renew reply") {
            RenewReply::Invoice(inv) => {
                assert_eq!(inv.request_id.as_deref(), Some("req-0"));
                assert_eq!(inv.bolt11, "lnbcrenew1");
            }
            RenewReply::Retry(n) => {
                panic!("a stored notice must not preempt this attempt's invoice: {n:?}")
            }
        }
        assert_eq!(
            relay.grace_windows_entered(),
            1,
            "the pinned path holds the notice and keeps reading"
        );
    }

    // Same race, other carrier: the previous attempt's `order.error` refusal is stored under the
    // pinned id (lnrent-dvb) and lands ahead of the live invoice the repointed daemon just minted.
    // Reporting the stale refusal would turn a successful renewal into exit 6.
    #[tokio::test]
    async fn pinned_renew_prefers_a_live_invoice_over_a_stored_order_error() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), refusal("req-0")).await);
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("renew reply") {
            RenewReply::Invoice(inv) => assert_eq!(inv.request_id.as_deref(), Some("req-0")),
            RenewReply::Retry(n) => panic!("expected the live invoice, got {n:?}"),
        }
        assert_eq!(relay.grace_windows_entered(), 1);
    }

    // The honest-refusal half, and the bound. The ONLY reply is a RESUMING notice; an invoice would
    // arrive later, but only after the grace window has closed. The notice must come back — not be
    // swallowed, and not held until the full request deadline. With an unbounded window the stream
    // would go on to deliver the late invoice and this returns `Invoice` instead.
    #[tokio::test]
    async fn pinned_renew_returns_a_lone_resuming_notice_within_the_grace_window() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-0"))).await);
        relay.queue_late(
            reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await,
        );

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("the notice is a real answer") {
            RenewReply::Retry(n) => assert_eq!(n.state, "RESUMING"),
            RenewReply::Invoice(inv) => {
                panic!("the grace window ran past its deadline and read a late reply: {inv:?}")
            }
        }
        assert_eq!(relay.grace_windows_entered(), 1);
    }

    // Same bound for the refusal carrier: a lone `order.error` is still the operator's answer and
    // still surfaces as its own structured error (exit 6), one grace window later.
    #[tokio::test]
    async fn pinned_renew_returns_a_lone_order_error_within_the_grace_window() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), refusal("req-0")).await);
        relay.queue_late(
            reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await,
        );

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .renew("sub-1")
            .await
            .expect_err("a refusal is not a renewal");
        assert_eq!(err.exit_code(), 6, "remote error, NOT the timeout's 4");
        assert_eq!(err.envelope().code, "unavailable");
        assert_eq!(relay.grace_windows_entered(), 1);
    }

    // The default path must not pay for any of this. A freshly minted id has never been published,
    // so no stored reply can exist under it: the first acceptable reply IS the answer and comes back
    // with no added latency. Fails if the grace window is applied unconditionally — the window would
    // read on to the invoice and return that instead.
    #[tokio::test]
    async fn unpinned_renew_returns_its_first_acceptable_reply_with_no_grace_window() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), resuming_notice("sub-1", Some("req-0"))).await);
        relay.queue(reply(&op, &buyer.public_key(), billing_invoice("sub-1", Some("req-0"))).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        match c.renew("sub-1").await.expect("the first reply answers") {
            RenewReply::Retry(n) => assert_eq!(n.state, "RESUMING"),
            RenewReply::Invoice(inv) => {
                panic!("a fresh id must not enter the grace window: {inv:?}")
            }
        }
        assert_eq!(
            relay.grace_windows_entered(),
            0,
            "a fresh id has no stored reply to race, so no deadline is shortened"
        );
    }

    // The costly carrier (lnrent-1jm's worst case). The pinned id's first attempt was refused
    // row-free (`not_active` — nothing persisted, `daemon/src/op_dispatch.rs:96-108`); the operator
    // fixed the condition, the re-sent id RAN the hook, and that stored refusal is still on the
    // relay. Reporting it would send the buyer back with a fresh id and run a non-idempotent op a
    // second time.
    #[tokio::test]
    async fn pinned_op_prefers_a_live_ok_result_over_a_stored_refusal() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), op_reject("req-0")).await);
        relay.queue(reply(&op, &buyer.public_key(), op_ok("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let got = c
            .invoke_op("sub-1", "restart", Some("request"), json!({}))
            .await
            .expect("the hook ran and succeeded");

        assert_eq!(got.status, OpStatus::Ok);
        assert_eq!(got.data.unwrap()["restarted"], true);
        assert_eq!(relay.grace_windows_entered(), 1);
    }

    // The ops honest-refusal half, and its bound: a genuine reject is still reported as a reject
    // (exit 6), one grace window later — never swallowed, never held to the request deadline. An
    // unbounded window would read the late `ok` and report success for an op that was refused.
    #[tokio::test]
    async fn pinned_op_returns_a_lone_refusal_within_the_grace_window() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), op_reject("req-0")).await);
        relay.queue_late(reply(&op, &buyer.public_key(), op_ok("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .invoke_op("sub-1", "restart", Some("request"), json!({}))
            .await
            .expect_err("the refusal is the answer");

        assert_eq!(err.exit_code(), 6);
        assert_eq!(err.envelope().code, "not_active");
        assert_eq!(relay.grace_windows_entered(), 1);
    }

    // The ops default path, unchanged: a fresh id's first `op.result` is its answer, error or not.
    #[tokio::test]
    async fn unpinned_op_returns_its_first_result_with_no_grace_window() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::default();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), op_reject("req-0")).await);
        relay.queue(reply(&op, &buyer.public_key(), op_ok("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .invoke_op("sub-1", "restart", Some("request"), json!({}))
            .await
            .expect_err("the first result answers");

        assert_eq!(err.envelope().code, "not_active");
        assert_eq!(relay.grace_windows_entered(), 0);
    }

    // `order.request` is deliberately NOT a carrier: the daemon caches its reply — invoice or error
    // — on `(sender, request_id)` and resends that same row for a duplicate
    // (`daemon/src/order_intake.rs:144-149,760-773`), so the stored copy and the live answer agree
    // and there is nothing to prefer. A pinned order create must therefore return its refusal
    // immediately, with no grace window at all.
    #[tokio::test]
    async fn pinned_order_create_returns_its_refusal_with_no_grace_window() {
        let op = Keys::generate();
        let buyer = Keys::generate();
        let clock = TestClock::pinned();
        let relay = FakeRelay::new();
        relay.queue(reply(&op, &buyer.public_key(), refusal("req-0")).await);

        let c = client(&relay, &buyer, &clock, op.public_key());
        let err = c
            .create_order("30402:op:dummy", json!({}), None)
            .await
            .unwrap_err();

        assert_eq!(err.exit_code(), 6);
        assert_eq!(err.envelope().code, "unavailable");
        assert_eq!(
            relay.grace_windows_entered(),
            0,
            "a cached-reply carrier must not pay the grace window"
        );
    }
}
