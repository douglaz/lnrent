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

/// The outcome of [`BuyerClient::renew`] (lnrent-zs2). A renewal request against an
/// ACTIVE/SUSPENDED sub is answered by a request-correlated `billing.invoice` (`Invoice`); a
/// request that lands while the sub is transiently RESUMING is answered by a request-correlated
/// `billing.notice` asking the buyer to retry once the resume completes (`Retry`, lnrent-z4u). Both
/// carry this request's `request_id`, so the `renew` matcher accepts either and the RESUMING case
/// surfaces as operator feedback instead of a timeout — while a relay-replayed stale notice from an
/// earlier request (different id) is ignored.
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
    /// request's `request_id`, so the notice is accepted immediately (no invoice is coming) and a
    /// relay-replayed stale RESUMING notice from an earlier request — carrying a different id —
    /// cannot masquerade as this request's reply. An otherwise-invalid renewal (unknown sub /
    /// non-owner / non-renewable state) is dropped by the operator with no reply, surfacing here as
    /// a timeout.
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
                    return false;
                }
                match m {
                    // The renewal invoice answering THIS request (request_id + sub correlated).
                    Msg::BillingInvoice(bi) => {
                        bi.request_id.as_deref() == Some(&want_id) && bi.subscription_id == want_sub
                    }
                    // The transient-RESUMING answer to THIS request (lnrent-z4u/zs2): the daemon
                    // replies with a request-correlated billing.notice INSTEAD of an invoice, so
                    // accept it immediately — no invoice is coming for this request. request_id
                    // correlation excludes a relay-replayed stale RESUMING notice from an earlier
                    // request (its id differs), and only state "RESUMING" qualifies (the operator
                    // emits same-sub notices for ACTIVE/SUSPENDED/CANCELLED too).
                    Msg::BillingNotice(n) => {
                        n.request_id.as_deref() == Some(&want_id)
                            && n.subscription_id == want_sub
                            && n.state == "RESUMING"
                    }
                    _ => false,
                }
            })
            .await?;
        self.check_sender(&sender, "renew reply")?;
        match reply {
            Msg::BillingInvoice(invoice) => Ok(RenewReply::Invoice(invoice)),
            Msg::BillingNotice(notice) => Ok(RenewReply::Retry(notice)),
            _ => unreachable!(
                "the matcher restricts to a request-correlated billing.invoice or RESUMING billing.notice"
            ),
        }
    }

    /// Invoke a management operation: send `op.request` and await the `op.result` correlated by
    /// `request_id`. An `interactive`-kind op is rejected up-front with `unsupported_interactive`
    /// (Iroh sessions are out of scope for M1a, §9.2) without sending anything. An `op.result`
    /// error becomes a `Remote` error (exit 6); an `ok` result is returned for the caller to render.
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
                sender == &operator
                    && matches!(m, Msg::OpResult(r)
                        if r.request_id == want_id
                            && r.subscription_id == want_sub
                            && r.op == want_op)
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
        loop {
            match stream.next().await.map_err(transport)? {
                None => {
                    return Err(BuyerError::Timeout(
                        "no correlated reply from the operator before the deadline".into(),
                    ))
                }
                Some(event) => {
                    // A gift wrap that won't unwrap (not for us / undecodable) is skipped, not fatal.
                    let Ok(unwrapped) = gift_unwrap(self.signer, &event).await else {
                        continue;
                    };
                    if want(&unwrapped.sender, &unwrapped.msg) {
                        return Ok((unwrapped.sender, unwrapped.msg));
                    }
                }
            }
        }
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

    /// A deterministic clock + counter-based request ids so a test can pre-build the matching reply.
    #[derive(Default)]
    struct TestClock {
        n: AtomicU64,
    }
    impl Clock for TestClock {
        fn now_secs(&self) -> i64 {
            1_000
        }
        fn new_request_id(&self) -> String {
            format!("req-{}", self.n.fetch_add(1, Ordering::SeqCst))
        }
    }

    /// An in-memory relay: `listings` answers discovery; `replies` is drained into the gift-wrap
    /// stream when a flow subscribes; `published` records what the buyer sent.
    struct FakeRelay {
        listings: Vec<Event>,
        replies: Mutex<VecDeque<Event>>,
        published: Mutex<Vec<Event>>,
    }
    impl FakeRelay {
        fn new() -> Self {
            Self {
                listings: Vec::new(),
                replies: Mutex::new(VecDeque::new()),
                published: Mutex::new(Vec::new()),
            }
        }
        fn queue(&self, event: Event) {
            self.replies.lock().unwrap().push_back(event);
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
            Ok(Box::new(FakeStream { events }))
        }
    }
    struct FakeStream {
        events: VecDeque<Event>,
    }
    #[async_trait]
    impl GiftWrapStream for FakeStream {
        async fn next(&mut self) -> Result<Option<Event>, RelayError> {
            Ok(self.events.pop_front())
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
}
