//! Order intake + invoice issuance (lnrent-7fp.17, SPEC.md §6.6, ADR-0009 §6.6).
//!
//! The concrete [`OrderHandler`] the Nostr engine (lnrent-7fp.5) routes buyer→operator order /
//! billing DMs to. It only *consumes* the existing seams — it does not rebuild transport,
//! payment, reservation, or capture:
//! - `inbound_request` idempotency on `(sender_pubkey, request_id)` (§5.1): a duplicate resends
//!   the cached response and never opens a second order;
//! - param + price validation via [`reservation::validate_params`] and the current `listing` row;
//! - order-time capacity via [`reservation::reserve`] / [`reservation::release`] (lnrent-7fp.7);
//! - a deterministic `external_id` + the idempotent [`PaymentBackend::create_invoice`];
//! - the one-transaction multi-row write the same way [`crate::capture`] does it: the PENDING
//!   subscription + the OPEN invoice + the cached `inbound_request` response all commit together,
//!   and the DM is sent only after commit.
//!
//! On any failure between validation and commit it sends a structured `order.error` and releases
//! the reservation, leaving no dangling PENDING subscription.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};

use lnrent_wire::{
    BillingInvoice, BillingNotice, Msg, OrderError, OrderInvoice, OrderRequest, PublicKey,
    RenewRequest, SubCancel, WireError,
};

use crate::backends::PaymentBackend;
use crate::clock::Clock;
use crate::nostr_engine::{OrderHandler, Outbound};
use crate::recipe::Recipe;
use crate::refund_resolver::{detect_form, validate_dest_format, DestForm};
use crate::reservation::{self, Budget, Request, Reserve};
use crate::store::Store;

/// Lightning expiry stamped on a first-order / renewal invoice (seconds). The order's capacity
/// reservation is held until this same horizon, then released (§9.3). An internal default, not an
/// operator config knob (scope: lnrent-7fp.17).
const INVOICE_EXPIRY_S: u32 = 3600;
const MIN_RENEWAL_INVOICE_EXPIRY_S: i64 = 60;

/// The stateless informational `billing.notice` message an owner gets when their `sub.cancel`
/// or `renew.request` lands while the subscription is transiently `RESUMING` (lnrent-z4u,
/// option (a)). Shared so the cancel and renew branches cannot drift apart.
const RESUMING_RETRY_NOTICE: &str = "a renewal is being applied — please retry in a moment";

/// The `unavailable` message a `renew.request` gets when the subscription belongs to a recipe this
/// daemon does not serve (lnrent-dvb). Deliberately says only that service for this subscription is
/// not offered HERE: it does not name — or hint at the existence of — whatever else the operator
/// runs, and it does not call the subscription invalid, because it is not.
const FOREIGN_RECIPE_REFUSAL: &str = "this subscription is not currently served here";

/// The order-intake integrator: implements [`OrderHandler`] over the injected store, payment
/// backend, clock, recipe, and host budget. Cheap to share behind an `Arc` (the engine holds it
/// as `Arc<dyn OrderHandler>`).
pub struct OrderIntake {
    store: Store,
    payment: Arc<dyn PaymentBackend>,
    clock: Arc<dyn Clock>,
    /// The recipe this operator serves (M1a is single-recipe). Provides the param schema, the
    /// reserved resources, the wire `period` string, and the authoritative current price.
    recipe: Recipe,
    /// The host's rentable budget for the capacity reservation (§9.3).
    budget: Budget,
    /// Per-pubkey anti-griefing cap: max concurrent LIVE HELD holds one buyer key may hold (PR-1).
    max_live_holds_per_buyer: u32,
}

/// The fields a buyer `renew.request` is authorized and priced against, read from the
/// `subscription` row by [`OrderIntake::load_renewable`]. Named rather than a 6-tuple for the same
/// reason [`ListingRow`] is: two of the columns are bare `Option<i64>` deadlines that a positional
/// destructure would happily swap.
struct RenewableRow {
    buyer_hex: String,
    state: String,
    paid_through: Option<i64>,
    retention_s: i64,
    suspend_not_before: Option<i64>,
    /// The recipe this subscription was ORDERED under; `None` for an unowned/legacy row.
    recipe_id: Option<String>,
}

/// The fields the order path needs from the current `listing` row (§5.4): the published price +
/// the per-listing timers copied onto the subscription at order time.
struct ListingRow {
    recipe_id: Option<String>,
    amount_sat: i64,
    period_s: i64,
    renew_lead_s: i64,
    retention_s: i64,
    state: String,
}

impl OrderIntake {
    pub fn new(
        store: Store,
        payment: Arc<dyn PaymentBackend>,
        clock: Arc<dyn Clock>,
        recipe: Recipe,
        budget: Budget,
        max_live_holds_per_buyer: u32,
    ) -> Self {
        Self {
            store,
            payment,
            clock,
            recipe,
            budget,
            max_live_holds_per_buyer,
        }
    }

    /// The `order.request` flow (SPEC.md §6.6 ordering): dedup → request-id gate →
    /// refund-dest gate → validate → reserve → invoice → one-transaction write → send after commit.
    async fn handle_order(
        &self,
        sender: PublicKey,
        req: OrderRequest,
        out: &dyn Outbound,
    ) -> Result<()> {
        // 1. DEDUP on (sender, request_id): resend the cached response and STOP — never open a 2nd
        //    order (§5.1).
        if let Some(cached) = self.cached_response(&sender, &req.id).await? {
            out.reply(&sender, &cached).await?;
            return Ok(());
        }
        if let Err(error) = validate_buyer_request_id_tail(&req.id) {
            return self.fail_order(&sender, &req.id, None, error, out).await;
        }

        let now = self.clock.now();
        let sender_hex = sender.to_hex();
        let order_id = format!("ord:{sender_hex}:{}", req.id);

        // 2a. REQUIRE a re-resolvable refund route BEFORE params/reservation/invoice/subscription
        //     work. Raw BOLT11 is single-use and may be expired by refund time; BOLT12 is the future
        //     re-resolvable single-string option once supported. This is a permanent request error,
        //     carrying no order_id and leaving no dangling state.
        if let Err(error) = validate_new_order_refund_dest(req.refund_dest.as_deref()) {
            return self.fail_order(&sender, &req.id, None, error, out).await;
        }

        // 2b. VALIDATE params against the recipe (§7.1). A pre-order failure carries NO order_id.
        let Some(params_obj) = req.params.as_object() else {
            return self
                .fail_order(
                    &sender,
                    &req.id,
                    None,
                    params_invalid("order params must be a JSON object"),
                    out,
                )
                .await;
        };
        if let Err(e) = reservation::validate_params(&self.recipe, params_obj) {
            return self
                .fail_order(&sender, &req.id, None, params_invalid(&e.to_string()), out)
                .await;
        }

        // 2c. PRICE check: the referenced listing must still be the current, ACTIVE one for this
        //     recipe at the published price — a stale/unknown price is `price_changed` (§5.4).
        //
        //     A listing that is not ACTIVE is reported as `unavailable`, NOT `price_changed`
        //     (lnrent-i23): the Operator has not published it, or has retracted it — the price did
        //     not move, and telling a Buyer their price is stale invites them to re-quote against
        //     something that is not being sold at all. Before i23 no code path ever wrote a
        //     non-ACTIVE state, so this arm was unreachable and the conflation cost nothing; the
        //     publication gate and the withdraw verb make it the state every install boots into.
        let listing = self.load_listing(&req.listing_id).await?;
        if let Some(l) = &listing {
            if l.state != "ACTIVE" {
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        None,
                        unavailable("this listing is not currently offered"),
                        out,
                    )
                    .await;
            }
        }
        let stale = match &listing {
            None => true,
            Some(l) => {
                l.recipe_id.as_deref() != Some(self.recipe.service.id.as_str())
                    || l.amount_sat != self.recipe.pricing.amount_sat as i64
            }
        };
        if stale {
            return self
                .fail_order(&sender, &req.id, None, price_changed(), out)
                .await;
        }
        let listing = listing.expect("stale=false implies a listing row");

        // 3. RESERVE capacity atomically (§9.3). CapacityFull is a normal business result.
        let reservation_id = format!("res:{sender_hex}:{}", req.id);
        let reserve_req = Request {
            resources: self.recipe.provisioning.resources.clone(),
            ports: 0,
        };
        let expires_at = now + i64::from(INVOICE_EXPIRY_S);
        match reservation::reserve(
            &self.store,
            &reservation_id,
            &order_id,
            reserve_req,
            self.budget,
            expires_at,
            now,
            self.max_live_holds_per_buyer,
        )
        .await?
        {
            Reserve::CapacityFull => {
                return self
                    .fail_order(&sender, &req.id, Some(&order_id), capacity_full(), out)
                    .await;
            }
            Reserve::Reserved => {}
        }

        // 4. Deterministic external_id binds settlement → order (§6.6); create_invoice is
        //    idempotent on it, so a crash-retry regenerates the same invoice.
        let external_id = format!("order:{sender_hex}:{}", req.id);
        let amount_sat = listing.amount_sat as u64;
        let invoice = match self
            .payment
            .create_invoice(
                amount_sat,
                &format!("lnrent order {order_id}"),
                INVOICE_EXPIRY_S,
                &external_id,
            )
            .await
        {
            Ok(inv) => inv,
            Err(e) => {
                // No sub committed yet — release the HELD reservation, then a structured error.
                // The detail stays in the local log: backend/gateway internals must not reach an
                // unauthenticated stranger over the wire (mirror op_dispatch's redaction).
                tracing::warn!(order = %order_id, error = %e, "create_invoice failed for order");
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        Some(&order_id),
                        unavailable("payment backend unavailable"),
                        out,
                    )
                    .await;
            }
        };

        // (Invoice-expiry is enforced at SETTLEMENT, not issuance: comparing the backend's
        // invoice.expires_at to our clock here is fragile across clock sources, so capture rejects a
        // settlement at/after expiry instead — see lnrent-g5p.)

        // The response we will both cache and (after commit) send. order_id is known now.
        let response = Msg::OrderInvoice(OrderInvoice {
            request_id: req.id.clone(),
            order_id: order_id.clone(),
            bolt11: invoice.bolt11.clone(),
            // Use the RETURNED invoice's amount, not the current listing price: create_invoice is
            // idempotent on external_id, so a crash-retry (or reissue after a price edit) returns the
            // ORIGINAL invoice — the reply/DB amount must match its bolt11, never drift (codex pass 4).
            amount_sat: invoice.amount_sat,
            period: self.recipe.pricing.period.clone(),
            expires_at: invoice.expires_at,
        });
        let response_json = serde_json::to_string(&response)?;

        // 5. ONE transaction (the capture.rs atomic-multi-row style): PENDING sub + OPEN invoice +
        //    cached inbound_request response. Re-check the dedup key INSIDE the txn so a concurrent
        //    duplicate that slipped past step 1 commits exactly one order (the store actor
        //    serializes txns, so the loser sees the winner's row).
        // The refusal the two capacity-releasing branches cache in their OWN transaction. Built here
        // because it must be durable together with the release: `reservation::reserve` bails on a
        // `RELEASED` row, so a crash between the release and a separate cache write would leave
        // every relay redelivery of this request failing instead of resending an idempotent answer.
        let refusal_json = serde_json::to_string(&Msg::OrderError(OrderError {
            request_id: req.id.clone(),
            order_id: None,
            error: unavailable("this listing is not currently offered"),
        }))?;
        let owned = OrderWrite {
            refusal_json,
            sender_hex: sender_hex.clone(),
            request_id: req.id.clone(),
            order_id: order_id.clone(),
            recipe_id: self.recipe.service.id.clone(),
            listing_id: req.listing_id.clone(),
            buyer_hex: sender_hex.clone(),
            params_json: req.params.to_string(),
            refund_dest: req.refund_dest.clone(),
            period_s: listing.period_s,
            renew_lead_s: listing.renew_lead_s,
            retention_s: listing.retention_s,
            inv_id: invoice.id.clone(),
            external_id: external_id.clone(),
            backend_invoice_id: invoice.backend_invoice_id.clone(),
            payment_hash: invoice.payment_hash.clone(),
            bolt11: invoice.bolt11.clone(),
            amount_sat: invoice.amount_sat as i64,
            inv_expires_at: invoice.expires_at,
            response_json,
            now,
        };
        let committed = self.store.transaction(move |tx| owned.write(tx)).await;
        let winner = match committed {
            // The operator withdrew the listing while we were minting the invoice. Nothing was
            // written and the hold was released in that same transaction, so answer the Buyer the
            // same `unavailable` the §5.4 gate would have: from their side the listing simply is not
            // being offered. The minted invoice is abandoned unpaid and expires on its own — it was
            // never sent, so nobody can pay it (and capture would refuse it anyway: no subscription
            // row exists).
            Ok(OrderCommit::ListingGone) => {
                tracing::info!(
                    order = %order_id,
                    "listing was withdrawn while this order was minting its invoice; refusing it"
                );
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        None,
                        unavailable("this listing is not currently offered"),
                        out,
                    )
                    .await;
            }
            // A duplicate delivery of this same request already refused it and released the shared
            // hold. Answer identically rather than committing behind a dead reservation — from the
            // Buyer's side one `order.request` got one answer, which is what the dedup contract
            // promises.
            Ok(OrderCommit::HoldLost) => {
                tracing::info!(
                    order = %order_id,
                    "a duplicate delivery of this order already refused it and released the hold; \
                     refusing this one too rather than committing without capacity"
                );
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        None,
                        unavailable("this listing is not currently offered"),
                        out,
                    )
                    .await;
            }
            Ok(OrderCommit::Committed) => None,
            Ok(OrderCommit::Duplicate(json)) => Some(json),
            Err(e) => {
                // Same redaction as the create_invoice arm: sqlite/store internals stay in the log.
                tracing::warn!(order = %order_id, error = %e, "order commit failed");
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        Some(&order_id),
                        unavailable("temporary storage failure"),
                        out,
                    )
                    .await;
            }
        };

        // 6. AFTER commit, send order.invoice — ours, or a concurrent winner's cached response.
        let to_send = match winner {
            Some(json) => {
                let msg: Msg = serde_json::from_str(&json)
                    .context("decoding concurrent cached order response")?;
                // We reserved capacity but a concurrent same-id request won the idempotency row
                // with a NON-invoice (an error — e.g. a pre-order failure that had no hold of its
                // own to release). No order will consume our hold, so release it (codex pass 3 P2).
                if !matches!(msg, Msg::OrderInvoice(_)) {
                    reservation::release(&self.store, &order_id, now).await?;
                }
                msg
            }
            None => response,
        };
        out.reply(&sender, &to_send).await?;
        Ok(())
    }

    /// The buyer `renew.request` flow: dedup, then issue a renewal invoice with a deterministic
    /// `renew:req:<sender>:<request_id>` external_id and reply `billing.invoice` (§6.6).
    async fn handle_renew(
        &self,
        sender: PublicKey,
        req: RenewRequest,
        out: &dyn Outbound,
    ) -> Result<()> {
        if let Some(cached) = self.cached_response(&sender, &req.id).await? {
            out.reply(&sender, &cached).await?;
            return Ok(());
        }
        // Structural id gate (mi9.2/DRIFT-3, gate0-abuse-resistance §D): the same 1..=128 /
        // [A-Za-z0-9_-] tail bound the order path enforces, BEFORE the id reaches the renew
        // external id or the inbound_request row. Malformed → drop + log with NO reply: every renew
        // reply correlates by echoing this exact id back (`billing.invoice`, the z4u RESUMING notice,
        // the dvb refusal below), so an unvalidated id has nowhere to go — matching renew's other
        // rejects above/below.
        if validate_buyer_request_id_tail(&req.id).is_err() {
            tracing::warn!(sub = %req.subscription_id, "renew.request with a malformed request id — dropped");
            return Ok(());
        }
        let now = self.clock.now();
        // Authorize + gate state: only the OWNING buyer may renew, and only a renewable
        // (ACTIVE/SUSPENDED) subscription. Otherwise drop silently — an outsider must not be able
        // to mint a payable billing.invoice for someone else's sub, and a PENDING/terminal sub must
        // not get a renewal invoice that capture would later refund (§5.1 sender auth, §6.3).
        let Some(sub) = self.load_renewable(&req.subscription_id).await? else {
            tracing::warn!(sub = %req.subscription_id, "renew.request for unknown subscription — dropped");
            return Ok(());
        };
        if sub.buyer_hex != sender.to_hex() {
            tracing::warn!(sub = %req.subscription_id, "renew.request from a non-owner — dropped");
            return Ok(());
        }
        // lnrent-dvb: the BUYER-initiated mirror of lnrent-6id's `fire_soft_reminder` gate, sharing
        // its one ownership rule via `Recipe::owns_row` (NULL/absent owner is OWNED — gating NULL as
        // foreign was the yjl regression). `issue_renewal` prices unconditionally at
        // `self.recipe.pricing.amount_sat`, so a sub ordered under a DIFFERENT recipe would be quoted
        // OUR price and capture would extend ITS `paid_through` on payment: the operator eats the
        // difference when ours is cheaper, the buyer overpays when it is dearer. Gated BEFORE
        // `create_invoice` exactly as 6id is — create is idempotent on `external_id`, so a
        // wrong-priced bolt11 minted here would be handed back verbatim to any later renewal reusing
        // that id, including one issued by the recipe that does own the row. And AFTER the owner
        // check, so an outsider probing subscription ids still gets the silent drop.
        //
        // WHY THIS REPLY (§5.1 owns the vocabulary and records what a renew may be answered with).
        // 6id skips silently because no one asked it anything; this is a request. The CARRIER is
        // forced, not chosen — `order.error` is the wire's only error-bearing operator->buyer message
        // outside `op.result` — so only the CODE is a choice, and `unavailable` is the honest one:
        // the buyer did nothing wrong (`params_invalid` / `refund_dest_invalid` / `rejected` blame
        // them), their subscription is real, capacity is irrelevant (`capacity_full`), and the
        // message names no recipe so it leaks nothing about what else the operator runs. It is the
        // same code, and the same `retryable: true`, the order path sends for a listing it is not
        // offering (the listing-state check above) — a retry works the moment the operator repoints
        // this daemon. NOT `price_changed`, which IS what the order path answers a listing naming a
        // foreign recipe with: there a re-quote fixes it, here the price did not move and none can.
        //
        // Nothing durable is written, and the refusal is deliberately NOT cached in
        // `inbound_request` (§5.1 records the exception). That cache exists so a redelivery "never
        // creates a second reservation, order, or invoice"; this arm creates none of the three, and
        // leaving the `(sender, request_id)` key unclaimed means this daemon never owes a stale
        // refusal to a later request reusing that id — once it serves the owning recipe, that id
        // mints the renewal normally. The beneficiary is a buyer RE-SENDING their `--request-id`
        // (a fresh gift wrap): a true redelivery of the identical wrap never reaches here at all,
        // since returning `Ok(())` writes its `seen_message` row and the transport dedupe
        // short-circuits the replay (`nostr_engine`). `fail_order` must cache because ITS refusal
        // has to be durable together with releasing a HELD reservation, or a crash between the two
        // bricks every redelivery; this arm holds nothing.
        if !self.recipe.owns_row(sub.recipe_id.as_deref()) {
            tracing::warn!(
                sub = %req.subscription_id,
                row_recipe = sub.recipe_id.as_deref().unwrap_or(""),
                serving_recipe = %self.recipe.service.id,
                "renew.request for a different recipe — refused before minting an invoice"
            );
            // `retryable` answers "could this EVER succeed", not "would it succeed now": repointing
            // this daemon at the owning recipe only helps if the row can still reach a state the
            // renewal gate below accepts, so a row that cannot has an agent retrying a dead
            // subscription until it gives up. Set from an ALLOWLIST of the states that can still get
            // there (§6.3): PENDING/PROVISIONING become ACTIVE on payment+provision, RESUMING resolves
            // into ACTIVE, and ACTIVE/SUSPENDED are already accepted. The five omitted are dead ends —
            // TERMINATED/EXPIRED/CANCELLED/REFUNDED terminally, and REFUND_DUE because its only exit
            // is REFUNDED. Positive rather than a denylist so a state added later defaults to "makes
            // no promise": over-promising is the failure being fixed here, and this mirrors the
            // ACTIVE/SUSPENDED allowlist the state gate below already uses. Only the flag varies —
            // code and message stay uniform, because the REASON is identical in every state and a
            // per-state refusal would narrate the row's state back to the sender.
            //
            // STATE only, deliberately not the resumable boundary B the gate below also enforces.
            // Being past B looks terminal, but a repoint IS a daemon restart, and
            // `apply_restart_downtime_credit` credits exactly the row whose window the outage ate:
            // an ACTIVE candidate needs `effective_suspend_at >= last_heartbeat` and a SUSPENDED one
            // `B_old > last_heartbeat`, both satisfiable while `now >= B`, and each sets a floor
            // strictly AFTER `now` (`target = now + (lead - pre_available)`; `target_b = now +
            // remaining`). So the restart that repoints the daemon can itself reopen the window, and
            // `retryable: false` here would tell a buyer to abandon a rental the credit is designed
            // to give back — the costlier of the two errors, since state is durable but B is not.
            let mut error = unavailable(FOREIGN_RECIPE_REFUSAL);
            error.retryable = matches!(
                sub.state.as_str(),
                "PENDING" | "PROVISIONING" | "ACTIVE" | "RESUMING" | "SUSPENDED"
            );
            let response = Msg::OrderError(OrderError {
                request_id: req.id.clone(),
                order_id: None,
                error,
            });
            out.reply(&sender, &response).await?;
            return Ok(());
        }
        let RenewableRow {
            state,
            paid_through,
            retention_s,
            suspend_not_before,
            ..
        } = sub;
        // lnrent-z4u: see handle_cancel for the decision. A renew that lands while the sub is
        // transiently RESUMING gets the SAME stateless informational notice — this is not a
        // failure (the same request works once the resume lands), so BillingNotice is the right
        // channel; the dvb arm above borrows `order.error` precisely because a refusal IS one and
        // the wire has no renew-specific error type — owner-only (this branch sits after the owner
        // check) and with NO state change. Deliberately not a RESUMING->X
        // shortcut / resume-driver hook (the reverted P1 trap noted in handle_cancel).
        if state == "RESUMING" {
            // request_id echoes THIS renew.request so the buyer's `renew()` matches it exactly (like
            // a billing.invoice) — a relay-replayed stale RESUMING notice from an earlier request
            // carries a different id and cannot masquerade as this request's reply (lnrent-zs2).
            let notice = Msg::BillingNotice(BillingNotice {
                subscription_id: req.subscription_id.clone(),
                request_id: Some(req.id.clone()),
                state: "RESUMING".to_string(),
                message: RESUMING_RETRY_NOTICE.to_string(),
            });
            out.reply(&sender, &notice).await?;
            return Ok(());
        }
        if !matches!(state.as_str(), "ACTIVE" | "SUSPENDED") {
            tracing::warn!(sub = %req.subscription_id, %state, "renew.request for a non-renewable state — dropped");
            return Ok(());
        }
        // Past the CREDITED resumable boundary B = max(paid_through, suspend_not_before) +
        // retention_s the rental is effectively terminal even if reconcile hasn't flipped it yet —
        // and capture refunds settlements at/after that SAME boundary (the inclusive downtime-credit
        // gate in lnrent-7fp.8/§6.5). A downtime credit raises suspend_not_before above paid_through,
        // keeping the buyer resumable PAST the raw paid_through + retention_s; gating on raw
        // paid_through here would wrongly drop a renewal that capture would still accept (issuance and
        // capture must agree). Issuing a renewal invoice at/after B would only ever be refunded, never
        // applied, so drop it then (codex pass 3 P2; §6.3, §6.5). The paid_through math is unchanged:
        // due_at below stays anchored to paid_through, never the floor.
        let mut invoice_expiry_s = INVOICE_EXPIRY_S;
        if let Some(pt) = paid_through {
            let effective_suspend_at = pt.max(suspend_not_before.unwrap_or(pt));
            let resumable_until = effective_suspend_at + retention_s;
            if now >= resumable_until {
                tracing::warn!(sub = %req.subscription_id, "renew.request past the credited resumable window — dropped");
                return Ok(());
            }
            let remaining = resumable_until - now;
            if remaining < MIN_RENEWAL_INVOICE_EXPIRY_S {
                tracing::warn!(sub = %req.subscription_id, remaining_s = remaining, "renew.request too close to the credited resumable window boundary — dropped");
                return Ok(());
            }
            invoice_expiry_s = remaining.min(i64::from(INVOICE_EXPIRY_S)) as u32;
        }
        let due_at = paid_through.unwrap_or(now);
        let external_id = format!("renew:req:{}:{}", sender.to_hex(), req.id);
        let response = self
            .issue_renewal(
                &req.subscription_id,
                &external_id,
                Some(req.id.clone()),
                due_at,
                now,
                invoice_expiry_s,
                Some((&sender, &req.id)),
            )
            .await?;
        out.reply(&sender, &response).await?;
        Ok(())
    }

    /// The buyer `sub.cancel` flow: authorize by owner, then atomically mark the live/lapsing
    /// subscription `CANCELLED`, preserving the already-paid termination deadline.
    async fn handle_cancel(
        &self,
        sender: PublicKey,
        cancel: SubCancel,
        out: &dyn Outbound,
    ) -> Result<()> {
        let sub_id = cancel.subscription_id;
        let Some((buyer_hex, state)) = self.load_cancel_auth(&sub_id).await? else {
            tracing::warn!(sub = %sub_id, "sub.cancel for unknown subscription — dropped");
            return Ok(());
        };
        if buyer_hex != sender.to_hex() {
            tracing::warn!(sub = %sub_id, "sub.cancel from a non-owner — dropped");
            return Ok(());
        }
        // lnrent-z4u, option (a): a cancel that lands while the sub is transiently RESUMING gets a
        // STATELESS informational BillingNotice (better operability — the product surface), not the
        // old silent drop. This branch sits AFTER the owner check and BEFORE the state gate, so it is
        // owner-only: a non-owner still hits the silent drop above and never learns the sub exists.
        // We deliberately do NOT shortcut RESUMING -> CANCELLED, queue a pending cancel, or hook the
        // resume driver: any such state write would race/bypass the resume driver's CAS on
        // state='RESUMING' (resume.rs) — a reverted P1 trap. Cancel/renew are made to *function*
        // during RESUMING by NOTHING here; this is UX only. Other non-actionable states keep the
        // silent drop below.
        if state == "RESUMING" {
            // cancel is fire-and-forget (no request correlation), so this notice carries no
            // request_id — it reaches the buyer as an unsolicited async DM (zs2 targets the renew
            // path, where the client awaits and needs the request-correlated notice).
            let notice = Msg::BillingNotice(BillingNotice {
                subscription_id: sub_id.clone(),
                request_id: None,
                state: "RESUMING".to_string(),
                message: RESUMING_RETRY_NOTICE.to_string(),
            });
            out.reply(&sender, &notice).await?;
            return Ok(());
        }
        if !matches!(state.as_str(), "ACTIVE" | "SUSPENDED") {
            tracing::warn!(sub = %sub_id, %state, "sub.cancel for a non-cancellable state — dropped");
            return Ok(());
        }

        let notice = Msg::BillingNotice(BillingNotice {
            subscription_id: sub_id.clone(),
            request_id: None,
            state: "CANCELLED".to_string(),
            message:
                "subscription cancelled; service runs until the paid period ends, then terminates"
                    .to_string(),
        });
        let write = CancelWrite {
            sub_id,
            buyer_hex,
            notice_json: serde_json::to_string(&notice)?,
            now: self.clock.now(),
        };
        self.store.transaction(move |tx| write.write(tx)).await?;
        Ok(())
    }

    /// Issue the daemon soft-date auto-renewal invoice for `subscription_id` (no buyer request),
    /// where `cycle_anchor` is the `paid_through` being renewed — so one cycle yields one invoice
    /// via the deterministic `renew:auto:<sub>:<cycle_anchor>` external_id (§6.6). Sends
    /// `billing.invoice` with no `request_id` to the subscription's buyer.
    ///
    /// TEST-ONLY seam (lnrent-ux7): this helper sizes its invoice to a FIXED `INVOICE_EXPIRY_S` (1h)
    /// and is NOT the production auto-renewal path. Production soft-date auto-renewals fire from
    /// [`crate::reconcile::Reconciler::fire_soft_reminder`], which sizes the invoice expiry to the
    /// downtime-CREDITED renewal window `B = effective_suspend_at + retention_s` (§6.5). The fixed
    /// expiry here is deliberate — this seam exists only to exercise the issuance/outbox plumbing, so
    /// it intentionally does not pull in the subscription's floor/retention reads that credited sizing
    /// needs. Gated `#[cfg(test)]` so the fixed-expiry helper cannot be reached from production and
    /// mistaken for the auto-renewal path; its sole caller is a unit test.
    #[cfg(test)]
    async fn issue_soft_date_renewal(
        &self,
        subscription_id: &str,
        cycle_anchor: i64,
        out: &dyn Outbound,
    ) -> Result<()> {
        let now = self.clock.now();
        let buyer = self.load_buyer(subscription_id).await?;
        let external_id = format!("renew:auto:{subscription_id}:{cycle_anchor}");
        let response = self
            .issue_renewal(
                subscription_id,
                &external_id,
                None,
                cycle_anchor,
                now,
                INVOICE_EXPIRY_S,
                None,
            )
            .await?;
        out.reply(&buyer, &response).await?;
        Ok(())
    }

    /// Shared renewal issuance: create the invoice (idempotent on `external_id`), persist the OPEN
    /// renewal invoice — and, for a buyer request, the cached `inbound_request` response — in one
    /// transaction, and return the `billing.invoice` message to send.
    #[allow(clippy::too_many_arguments)]
    async fn issue_renewal(
        &self,
        subscription_id: &str,
        external_id: &str,
        request_id: Option<String>,
        due_at: i64,
        now: i64,
        invoice_expiry_s: u32,
        dedupe: Option<(&PublicKey, &str)>,
    ) -> Result<Msg> {
        let amount_sat = self.recipe.pricing.amount_sat;
        let invoice = self
            .payment
            .create_invoice(
                amount_sat,
                &format!("lnrent renewal {subscription_id}"),
                invoice_expiry_s,
                external_id,
            )
            .await
            .context("creating renewal invoice")?;
        let response = Msg::BillingInvoice(BillingInvoice {
            subscription_id: subscription_id.to_string(),
            request_id,
            bolt11: invoice.bolt11.clone(),
            // The returned invoice's amount, not the current recipe price: a deterministic-external_id
            // reissue (esp. renew:auto:<sub>:<cycle_anchor>) returns the ORIGINAL invoice, so the
            // advertised/stored amount must track its bolt11, never the edited price (codex pass 4).
            amount_sat: invoice.amount_sat,
            due_at,
            expires_at: invoice.expires_at,
        });
        let owned = RenewalWrite {
            inv_id: invoice.id.clone(),
            subscription_id: subscription_id.to_string(),
            external_id: external_id.to_string(),
            backend_invoice_id: invoice.backend_invoice_id.clone(),
            payment_hash: invoice.payment_hash.clone(),
            bolt11: invoice.bolt11.clone(),
            amount_sat: invoice.amount_sat as i64,
            inv_expires_at: invoice.expires_at,
            dedupe: dedupe.map(|(s, r)| {
                (
                    s.to_hex(),
                    r.to_string(),
                    serde_json::to_string(&response).unwrap_or_default(),
                )
            }),
            now,
        };
        let cached = self.store.transaction(move |tx| owned.write(tx)).await?;
        match cached {
            Some(json) => {
                Ok(serde_json::from_str(&json)
                    .context("decoding cached renewal response on race")?)
            }
            None => Ok(response),
        }
    }

    /// Send `order.error` and release any HELD reservation for `release_order_id`, leaving no
    /// dangling PENDING sub. The error response is cached so a duplicate request resends it. In
    /// this flow the subscription row is created only at commit, so the error never carries an
    /// `order_id` (the wire field stays absent — §5.1).
    async fn fail_order(
        &self,
        sender: &PublicKey,
        request_id: &str,
        release_order_id: Option<&str>,
        error: WireError,
        out: &dyn Outbound,
    ) -> Result<()> {
        let now = self.clock.now();
        let response = Msg::OrderError(OrderError {
            request_id: request_id.to_string(),
            order_id: None,
            error,
        });
        // Cache the error FIRST; the cache insert is the idempotency arbiter (we resend the winner).
        let cached = self
            .cache_response_row(sender, request_id, "order", &response, now)
            .await?;
        let to_send = match cached {
            Some(c) => {
                serde_json::from_str(&c).context("decoding cached order response on race")?
            }
            None => response,
        };
        // Release the HELD reservation UNLESS an order.invoice owns it. Only a committed order keeps
        // the hold: if we won (our error), nothing committed; if a concurrent NON-invoice response
        // won (an error, or a cross-type reused id that cached a billing.invoice), no order will
        // consume the hold either — so release it (codex pass 6 P2; symmetric with the write-race
        // path). release is idempotent, so a double-release across racers is harmless.
        if !matches!(to_send, Msg::OrderInvoice(_)) {
            if let Some(order_id) = release_order_id {
                reservation::release(&self.store, order_id, now).await?;
            }
        }
        out.reply(sender, &to_send).await?;
        Ok(())
    }

    /// Read a cached `inbound_request` response for `(sender, request_id)`, decoded to a [`Msg`].
    async fn cached_response(&self, sender: &PublicKey, request_id: &str) -> Result<Option<Msg>> {
        let (s, r) = (sender.to_hex(), request_id.to_string());
        let row: Option<String> = self
            .store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT response_json FROM inbound_request WHERE sender_pubkey=?1 AND request_id=?2",
                    params![s, r],
                    |row| row.get(0),
                )
                .optional()?)
            })
            .await?;
        match row {
            Some(json) => Ok(Some(
                serde_json::from_str(&json).context("decoding cached inbound_request response")?,
            )),
            None => Ok(None),
        }
    }

    /// Cache a standalone response row (used for the error paths, which write no sub/invoice).
    /// `ON CONFLICT DO NOTHING` keeps the first cached answer; returns `Some(cached_json)` when a
    /// concurrent duplicate already cached a response (so the caller resends THAT, not its freshly
    /// built one — the idempotency contract, §5.1), else `None`.
    async fn cache_response_row(
        &self,
        sender: &PublicKey,
        request_id: &str,
        kind: &str,
        msg: &Msg,
        now: i64,
    ) -> Result<Option<String>> {
        let (s, r, k, mt, json) = (
            sender.to_hex(),
            request_id.to_string(),
            kind.to_string(),
            msg.type_str().to_string(),
            serde_json::to_string(msg)?,
        );
        self.store
            .transaction(move |tx| {
                let n = tx.execute(
                    "INSERT INTO inbound_request
                        (sender_pubkey, request_id, kind, response_msg_type, response_json, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(sender_pubkey, request_id) DO NOTHING",
                    params![s, r, k, mt, json, now],
                )?;
                if n > 0 {
                    return Ok(None); // we cached ours
                }
                // Lost the race: return the already-cached response to resend.
                Ok(tx
                    .query_row(
                        "SELECT response_json FROM inbound_request WHERE sender_pubkey=?1 AND request_id=?2",
                        params![s, r],
                        |row| row.get(0),
                    )
                    .optional()?)
            })
            .await
    }

    async fn load_listing(&self, listing_id: &str) -> Result<Option<ListingRow>> {
        let id = listing_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT recipe_id, amount_sat, period_s, renew_lead_s, retention_s, state
                     FROM listing WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok(ListingRow {
                            recipe_id: r.get(0)?,
                            amount_sat: r.get::<_, Option<i64>>(1)?.unwrap_or(0),
                            period_s: r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                            renew_lead_s: r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                            retention_s: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                            state: r.get::<_, Option<String>>(5)?.unwrap_or_default(),
                        })
                    },
                )
                .optional()?)
            })
            .await
    }

    /// The fields a buyer renewal must be authorized against, if the subscription exists, else
    /// `None`. `suspend_not_before` is the downtime-credit FLOOR (§6.5); it widens the renewal
    /// eligibility window the same way it widens capture's resumable boundary. `recipe_id` is the
    /// row's OWNING recipe — the renewal is priced at ours, so `handle_renew` must be able to see
    /// that it is somebody else's (lnrent-dvb); `None` there means an unowned/legacy row.
    async fn load_renewable(&self, sub_id: &str) -> Result<Option<RenewableRow>> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT buyer_pubkey, state, paid_through, retention_s, suspend_not_before,
                            recipe_id
                     FROM subscription WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok(RenewableRow {
                            buyer_hex: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            state: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            paid_through: r.get::<_, Option<i64>>(2)?,
                            retention_s: r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                            suspend_not_before: r.get::<_, Option<i64>>(4)?,
                            recipe_id: r.get::<_, Option<String>>(5)?,
                        })
                    },
                )
                .optional()?)
            })
            .await
    }

    /// Immutable owner plus a rough state gate for `sub.cancel`; the deadline is re-read inside the
    /// cancel transaction because renewals/credits can move it concurrently.
    async fn load_cancel_auth(&self, sub_id: &str) -> Result<Option<(String, String)>> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT buyer_pubkey, state FROM subscription WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok((
                            r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        ))
                    },
                )
                .optional()?)
            })
            .await
    }

    // Only reached by the `#[cfg(test)]` `issue_soft_date_renewal` seam above, so it is test-gated
    // too (else it is dead code in production builds). The credit-aware `renew.request` path resolves
    // its buyer inline via `load_renewable`.
    #[cfg(test)]
    async fn load_buyer(&self, sub_id: &str) -> Result<PublicKey> {
        let id = sub_id.to_string();
        let hex: Option<String> = self
            .store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT buyer_pubkey FROM subscription WHERE id = ?1",
                    params![id],
                    |r| r.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten())
            })
            .await?;
        let hex =
            hex.ok_or_else(|| anyhow::anyhow!("subscription {sub_id} has no buyer to renew for"))?;
        PublicKey::from_hex(&hex).context("parsing subscription buyer pubkey")
    }
}

#[async_trait]
impl OrderHandler for OrderIntake {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()> {
        match msg {
            Msg::OrderRequest(req) => self.handle_order(sender, req, out).await,
            Msg::RenewRequest(req) => self.handle_renew(sender, req, out).await,
            Msg::SubCancel(req) => self.handle_cancel(sender, req, out).await,
            // delivery.resend.request is routed here by the engine but owned by the supervisor's
            // delivery wrapper (lnrent-7fp.10).
            _ => Ok(()),
        }
    }
}

/// Owned inputs for the atomic order write, so the transaction closure is `move + 'static`.
struct OrderWrite {
    /// The `order.error` to cache ATOMICALLY with a refusal branch (`ListingGone` / `HoldLost`).
    /// Pre-built by the caller because those branches release capacity, and a crash between that
    /// release and a separate cache write would leave a request that can never be answered:
    /// `reservation::reserve` deliberately bails on a `RELEASED` row, so every relay redelivery
    /// would fail instead of resending an idempotent refusal. Caching it in the SAME transaction
    /// makes the release and its answer one durable fact.
    refusal_json: String,
    sender_hex: String,
    request_id: String,
    order_id: String,
    recipe_id: String,
    listing_id: String,
    buyer_hex: String,
    params_json: String,
    refund_dest: Option<String>,
    period_s: i64,
    renew_lead_s: i64,
    retention_s: i64,
    inv_id: String,
    external_id: String,
    backend_invoice_id: String,
    payment_hash: String,
    bolt11: String,
    amount_sat: i64,
    inv_expires_at: i64,
    response_json: String,
    now: i64,
}

impl OrderWrite {
    /// PENDING sub + OPEN invoice + cached response in one txn. Returns `Some(json)` if a
    /// concurrent duplicate already committed the order (its cached response to resend), else
    /// `None` (we committed).
    /// Cache the pre-built `order.error` for this request, in the caller's transaction. Same shape
    /// and same `DO NOTHING` conflict rule as `cache_response_row`, so a concurrent winner's cached
    /// answer is never clobbered — `fail_order` then reads it back and resends that winner.
    fn cache_refusal(&self, tx: &rusqlite::Transaction) -> Result<()> {
        tx.execute(
            "INSERT INTO inbound_request
                (sender_pubkey, request_id, kind, response_msg_type, response_json, created_at)
             VALUES (?1, ?2, 'order', 'order.error', ?3, ?4)
             ON CONFLICT(sender_pubkey, request_id) DO NOTHING",
            params![
                self.sender_hex,
                self.request_id,
                self.refusal_json,
                self.now
            ],
        )?;
        Ok(())
    }

    fn write(self, tx: &rusqlite::Transaction) -> Result<OrderCommit> {
        let existing: Option<String> = tx
            .query_row(
                "SELECT response_json FROM inbound_request WHERE sender_pubkey=?1 AND request_id=?2",
                params![self.sender_hex, self.request_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(json) = existing {
            return Ok(OrderCommit::Duplicate(json));
        }
        // RE-CHECK the listing INSIDE the txn (lnrent-i23). The §5.4 check upstream ran before
        // `create_invoice`, which is a network round-trip to the payment backend — seconds during
        // which `lnrent listing withdraw` can commit `WITHDRAWN`. Without this the order would still
        // commit a PENDING subscription and an OPEN invoice AFTER the operator's stop, handing a
        // Buyer a payable invoice for a listing that is no longer offered. The store actor
        // serializes transactions, so reading it here is atomic against that write: withdraw either
        // lands before this txn (and is seen) or after it (and the order was already accepted).
        // Same discipline as lnrent-gdu.3, which moved authorization ahead of the durable claim.
        let still_active: Option<String> = tx
            .query_row(
                "SELECT state FROM listing WHERE id=?1",
                params![self.listing_id],
                |r| r.get(0),
            )
            .optional()?;
        if still_active.as_deref() != Some("ACTIVE") {
            // Release the capacity hold in THIS transaction, so the refusal and the release commit
            // together — a hold left behind would keep a slot reserved for an order that will never
            // exist until its TTL lapsed.
            crate::reservation::release_txn(tx, &self.order_id, self.now)?;
            self.cache_refusal(tx)?;
            return Ok(OrderCommit::ListingGone);
        }
        // DEFENCE IN DEPTH, and deliberately kept as such: no order is ever committed without a
        // live hold behind it. The tail of this function only refreshes the hold's EXPIRY, never its
        // state, so committing on a `RELEASED` row would leave the slot counted free while a real
        // subscription occupied it — capacity handed out twice.
        //
        // HONEST REACHABILITY (this guard's original rationale is no longer true, and saying so
        // matters more than keeping the story tidy): it was added for a duplicate delivery whose
        // sibling had hit the branch above, released their SHARED reservation, and returned without
        // caching the refusal. Caching that refusal in the same transaction closed that path — the
        // dedup read at the top of this function now answers any later delivery as `Duplicate`
        // before it can reach here. No currently-reachable caller is known to trip this check; it
        // stays because the invariant is cheap to enforce and expensive to lose, and because a
        // future release path (a sweeper, a new refusal branch) would otherwise reopen the hazard
        // silently.
        let hold_state: Option<String> = tx
            .query_row(
                "SELECT state FROM reservation WHERE order_id=?1",
                params![self.order_id],
                |r| r.get(0),
            )
            .optional()?;
        if !matches!(hold_state.as_deref(), Some("HELD") | Some("CONSUMED")) {
            self.cache_refusal(tx)?;
            return Ok(OrderCommit::HoldLost);
        }
        // next_deadline = the invoice expiry: an unpaid PENDING order must be discoverable by the
        // reconcile `next_deadline <= now` cursor (lnrent-7fp.9) so it flips to EXPIRED at expiry —
        // otherwise the invoice stays OPEN and a late settlement would be captured/provisioned.
        tx.execute(
            "INSERT INTO subscription
                (id, recipe_id, listing_id, buyer_pubkey, state, params_json, refund_dest,
                 period_s, renew_lead_s, retention_s, next_deadline, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'PENDING', ?5, ?6, ?7, ?8, ?9, ?11, ?10, ?10)",
            params![
                self.order_id,
                self.recipe_id,
                self.listing_id,
                self.buyer_hex,
                self.params_json,
                self.refund_dest,
                self.period_s,
                self.renew_lead_s,
                self.retention_s,
                self.now,
                self.inv_expires_at,
            ],
        )?;
        tx.execute(
            "INSERT INTO invoice
                (id, subscription_id, external_id, backend_invoice_id, payment_hash, kind,
                 bolt11, amount_sat, status, expires_at, issued_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'order', ?6, ?7, 'OPEN', ?8, ?9)",
            params![
                self.inv_id,
                self.order_id,
                self.external_id,
                self.backend_invoice_id,
                self.payment_hash,
                self.bolt11,
                self.amount_sat,
                self.inv_expires_at,
                self.now,
            ],
        )?;
        tx.execute(
            "INSERT INTO inbound_request
                (sender_pubkey, request_id, kind, response_msg_type, response_json, created_at)
             VALUES (?1, ?2, 'order', 'order.invoice', ?3, ?4)",
            params![
                self.sender_hex,
                self.request_id,
                self.response_json,
                self.now
            ],
        )?;
        // Finalize the reservation TTL to the invoice's authoritative expiry (one expiry horizon,
        // §9.3) atomically with the commit. The hold was created at reserve-time with a provisional
        // TTL; the backend's `invoice.expires_at` — not our local clock — is the real horizon, so
        // align it here, where it can never diverge from the invoice/sub deadline (codex pass 2 P1).
        tx.execute(
            "UPDATE reservation SET expires_at = ?2 WHERE order_id = ?1",
            params![self.order_id, self.inv_expires_at],
        )?;
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, 'order_placed', ?2, ?3)",
            params![
                self.order_id,
                serde_json::json!({ "external_id": self.external_id }).to_string(),
                self.now,
            ],
        )?;
        Ok(OrderCommit::Committed)
    }
}

/// What [`OrderWrite::write`] did, inside the one serialized transaction.
enum OrderCommit {
    /// The order is committed: PENDING sub + OPEN invoice + cached response.
    Committed,
    /// A concurrent duplicate already committed this order; carries its cached response to resend.
    Duplicate(String),
    /// The listing stopped being `ACTIVE` between the §5.4 check and this commit — an operator's
    /// `listing withdraw` landing across `create_invoice`'s network round-trip. NOTHING was written.
    ListingGone,
    /// This order's capacity hold is no longer live. Committing anyway would put a subscription
    /// behind a `RELEASED` hold and let the slot be handed out twice, so NOTHING was written.
    ///
    /// A fail-closed invariant rather than a reachable race today: the path it was written for (a
    /// duplicate delivery whose sibling released the shared reservation) is now answered from the
    /// refusal cache before it gets here. See the comment at the check itself.
    HoldLost,
}

/// Owned inputs for the atomic renewal-invoice write.
struct RenewalWrite {
    inv_id: String,
    subscription_id: String,
    external_id: String,
    backend_invoice_id: String,
    payment_hash: String,
    bolt11: String,
    amount_sat: i64,
    inv_expires_at: i64,
    /// `(sender_hex, request_id, response_json)` for a buyer renew.request; `None` for a daemon
    /// soft-date renewal (nothing to dedupe).
    dedupe: Option<(String, String, String)>,
    now: i64,
}

impl RenewalWrite {
    /// Returns `Some(cached_json)` when a concurrent buyer renew.request for the same
    /// `(sender, request_id)` already cached a response (so the caller resends THAT, mirroring
    /// `OrderWrite`), else `None`.
    fn write(self, tx: &rusqlite::Transaction) -> Result<Option<String>> {
        // Dedup FIRST for a buyer renew: the (sender, request_id) key is SHARED with orders, so if a
        // response is already cached for it (e.g. a concurrent order committed first), resend THAT and
        // create NO renewal invoice — mirroring OrderWrite (codex pass 3 P2). The store actor
        // serializes txns, so this read is authoritative; a soft-date renewal (dedupe=None) skips it.
        if let Some((sender_hex, request_id, _)) = self.dedupe.as_ref() {
            if let Some(json) = tx
                .query_row(
                    "SELECT response_json FROM inbound_request WHERE sender_pubkey=?1 AND request_id=?2",
                    params![sender_hex, request_id],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
            {
                return Ok(Some(json));
            }
        }
        // Idempotent on external_id: re-issuing the same cycle never creates a 2nd invoice.
        tx.execute(
            "INSERT INTO invoice
                (id, subscription_id, external_id, backend_invoice_id, payment_hash, kind,
                 bolt11, amount_sat, status, expires_at, issued_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'renewal', ?6, ?7, 'OPEN', ?8, ?9)
             ON CONFLICT(external_id) DO NOTHING",
            params![
                self.inv_id,
                self.subscription_id,
                self.external_id,
                self.backend_invoice_id,
                self.payment_hash,
                self.bolt11,
                self.amount_sat,
                self.inv_expires_at,
                self.now,
            ],
        )?;
        if let Some((sender_hex, request_id, response_json)) = self.dedupe {
            tx.execute(
                "INSERT INTO inbound_request
                    (sender_pubkey, request_id, kind, response_msg_type, response_json, created_at)
                 VALUES (?1, ?2, 'renew', 'billing.invoice', ?3, ?4)
                 ON CONFLICT(sender_pubkey, request_id) DO NOTHING",
                params![sender_hex, request_id, response_json, self.now],
            )?;
        }
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, 'renew_invoice', ?2, ?3)",
            params![
                self.subscription_id,
                serde_json::json!({ "external_id": self.external_id }).to_string(),
                self.now,
            ],
        )?;
        Ok(None)
    }
}

/// Owned inputs for the atomic cancel write.
struct CancelWrite {
    sub_id: String,
    buyer_hex: String,
    notice_json: String,
    now: i64,
}

impl CancelWrite {
    /// Returns `true` when this call won the `ACTIVE`/`SUSPENDED -> CANCELLED` transition.
    fn write(self, tx: &rusqlite::Transaction) -> Result<bool> {
        let current: Option<(String, Option<i64>, Option<i64>)> = tx
            .query_row(
                "SELECT state, paid_through, next_deadline FROM subscription WHERE id = ?1",
                params![&self.sub_id],
                |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                        r.get(1)?,
                        r.get(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((state, paid_through, next_deadline)) = current else {
            return Ok(false);
        };
        let term_deadline = match state.as_str() {
            "ACTIVE" => paid_through,
            "SUSPENDED" => next_deadline,
            _ => return Ok(false),
        };
        let Some(term_deadline) = term_deadline else {
            return Ok(false);
        };

        let n = tx.execute(
            "UPDATE subscription SET state='CANCELLED', next_deadline=?2, updated_at=?3
             WHERE id=?1 AND state IN ('ACTIVE','SUSPENDED')",
            params![&self.sub_id, term_deadline, self.now],
        )?;
        if n == 0 {
            return Ok(false);
        }
        enqueue_outbox(
            tx,
            &format!("outbox:cancel-notice:{}:{term_deadline}", self.sub_id),
            &self.buyer_hex,
            &self.sub_id,
            "billing.notice",
            &self.notice_json,
            self.now,
        )?;
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, 'order_intake_cancel', ?2, ?3)",
            params![
                &self.sub_id,
                serde_json::json!({ "term_deadline": term_deadline }).to_string(),
                self.now,
            ],
        )?;
        Ok(true)
    }
}

/// Enqueue a buyer DM as a `PENDING` outbox row. Stable ids make retries idempotent.
#[allow(clippy::too_many_arguments)]
fn enqueue_outbox(
    tx: &rusqlite::Transaction,
    id: &str,
    recipient: &str,
    sub_id: &str,
    msg_type: &str,
    payload_json: &str,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outbox
            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, ?6)
         ON CONFLICT(id) DO NOTHING",
        params![id, recipient, sub_id, msg_type, payload_json, now],
    )?;
    Ok(())
}

// The five `order.error` codes (§5.1) — the only ones this handler emits. `retryable` follows the
// nature of the failure: a bad request is permanent, capacity / backend / store trouble is not.
pub(crate) fn validate_buyer_request_id_tail(id: &str) -> std::result::Result<(), WireError> {
    let valid_tail = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'));
    if valid_tail {
        Ok(())
    } else {
        Err(params_invalid(
            "request id must be 1..=128 chars using only [A-Za-z0-9_-]",
        ))
    }
}

fn validate_new_order_refund_dest(refund_dest: Option<&str>) -> std::result::Result<(), WireError> {
    let Some(dest) = refund_dest.map(str::trim).filter(|d| !d.is_empty()) else {
        return Err(refund_dest_invalid(
            "refund_dest is required; use a Lightning address or HTTPS LNURL",
        ));
    };

    match detect_form(dest).map_err(|e| refund_dest_invalid(&e.to_string()))? {
        DestForm::LnAddress { .. } | DestForm::Lnurl(_) => {
            validate_dest_format(dest).map_err(|e| refund_dest_invalid(&e.to_string()))
        }
        DestForm::Bolt11 => Err(refund_dest_invalid(
            "refund_dest must be re-resolvable (Lightning address or HTTPS LNURL), not a BOLT11 invoice",
        )),
    }
}

fn params_invalid(message: &str) -> WireError {
    WireError {
        code: "params_invalid".into(),
        message: message.into(),
        retryable: false,
    }
}
fn price_changed() -> WireError {
    WireError {
        code: "price_changed".into(),
        message: "listing price is no longer current; refetch the listing and reorder".into(),
        retryable: false,
    }
}
// refund_dest is missing, BOLT12, raw BOLT11, or malformed. A permanent request error: the buyer must
// resend a re-resolvable destination (a Lightning address or HTTPS LNURL).
fn refund_dest_invalid(message: &str) -> WireError {
    WireError {
        code: "refund_dest_invalid".into(),
        message: message.into(),
        retryable: false,
    }
}
fn capacity_full() -> WireError {
    WireError {
        code: "capacity_full".into(),
        message: "no capacity available for this order".into(),
        retryable: true,
    }
}
fn unavailable(message: &str) -> WireError {
    WireError {
        code: "unavailable".into(),
        message: message.into(),
        retryable: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::store::{migrate, Store};
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    };

    use crate::backends::{
        Invoice, MockPayment, PayStatus, PaymentBackend, PaymentStatus, Settlement,
    };
    use lnrent_wire::Keys;
    use nostr::EventId;
    use rusqlite::Connection;
    use serde_json::json;

    // Build via migrate() (not raw SCHEMA) so the store carries every applied migration — including
    // `subscription.suspend_not_before` (migration 3, §6.5), which the credited-renewal tests read.
    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        Store::spawn(conn)
    }

    fn dummy_recipe() -> Recipe {
        Recipe::load(format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR")))
            .expect("dummy recipe")
    }
    fn wireguard_recipe() -> Recipe {
        Recipe::load(format!(
            "{}/../recipes/wireguard",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("wireguard recipe")
    }

    /// A dummy-id recipe whose lifecycle hooks touch marker files, so cancel can prove it feeds the
    /// existing reconcile destroy path.
    fn marker_recipe() -> (Recipe, PathBuf, PathBuf) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "lnrent-order-intake-cancel-{}-{seq}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let suspend_marker = dir.join("suspended");
        let destroy_marker = dir.join("destroyed");
        std::fs::write(
            dir.join("suspend"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
                suspend_marker.display()
            ),
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
                destroy_marker.display()
            ),
        )
        .unwrap();
        for hook in ["suspend", "destroy"] {
            std::fs::set_permissions(dir.join(hook), std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }
        let mut recipe = dummy_recipe();
        recipe.dir = dir;
        (recipe, suspend_marker, destroy_marker)
    }

    /// The dummy recipe RE-PRICED, `service.id` unchanged so a seeded sub is still OWNED. Lets the
    /// lnrent-dvb tests assert the QUOTED amount against a number neither a seed helper nor another
    /// recipe fixture hard-codes, so a quote at some other recipe's price cannot pass by coincidence.
    fn priced_recipe(amount_sat: u64) -> Recipe {
        let mut r = dummy_recipe();
        r.pricing.amount_sat = amount_sat;
        r
    }

    fn budget_with_room() -> Budget {
        Budget {
            cpu: 4,
            mem_mb: 8192,
            disk_gb: 100,
            ports: 4,
        }
    }

    /// A stub [`Outbound`] that records every `(recipient, msg)` instead of touching a relay.
    #[derive(Default)]
    struct RecordingOutbound {
        sent: Mutex<Vec<(PublicKey, Msg)>>,
    }
    #[async_trait]
    impl Outbound for RecordingOutbound {
        async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
            self.sent.lock().unwrap().push((*recipient, msg.clone()));
            Ok(EventId::all_zeros())
        }
    }
    impl RecordingOutbound {
        fn messages(&self) -> Vec<(PublicKey, Msg)> {
            self.sent.lock().unwrap().clone()
        }
        fn only(&self) -> (PublicKey, Msg) {
            let mut m = self.messages();
            assert_eq!(m.len(), 1, "expected exactly one sent message, got {m:?}");
            m.pop().unwrap()
        }
    }

    fn intake(
        store: Store,
        payment: Arc<MockPayment>,
        clock: TestClock,
        recipe: Recipe,
        budget: Budget,
    ) -> OrderIntake {
        OrderIntake::new(store, payment, Arc::new(clock), recipe, budget, u32::MAX)
    }

    async fn seed_listing(store: &Store, id: &str, recipe_id: &str, amount_sat: i64) {
        seed_listing_in_state(store, id, recipe_id, amount_sat, "ACTIVE").await
    }

    /// The publication state is a parameter because it is load-bearing (lnrent-i23): `UNPUBLISHED`
    /// is what every fresh install is born in, so "a fresh daemon takes no orders" is a claim about
    /// THIS column meeting the §5.4 gate below.
    async fn seed_listing_in_state(
        store: &Store,
        id: &str,
        recipe_id: &str,
        amount_sat: i64,
        state: &str,
    ) {
        let (id, recipe_id, state) = (id.to_string(), recipe_id.to_string(), state.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO listing
                        (id, recipe_id, d_tag, amount_sat, period_s, renew_lead_s, retention_s, state, updated_at)
                     VALUES (?1, ?2, 'd', ?3, 2592000, 604800, 604800, ?4, 0)",
                    params![id, recipe_id, amount_sat, state],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// A backend that WITHDRAWS the listing as a side effect of minting the invoice — the operator's
    /// `listing withdraw` landing inside `create_invoice`'s network round-trip. That is the window
    /// the in-transaction re-check closes: the §5.4 gate has already passed by then, so without it
    /// the order commits a PENDING sub and an OPEN invoice AFTER the stop. Delegates the mint itself
    /// to `MockPayment` so the order reaches the commit exactly as it normally would.
    struct WithdrawDuringMint {
        inner: MockPayment,
        store: Store,
        listing_id: String,
    }

    #[async_trait::async_trait]
    impl PaymentBackend for WithdrawDuringMint {
        async fn create_invoice(
            &self,
            amount_sat: u64,
            memo: &str,
            expiry_s: u32,
            external_id: &str,
        ) -> Result<Invoice> {
            let id = self.listing_id.clone();
            self.store
                .transaction(move |tx| {
                    tx.execute(
                        "UPDATE listing SET state='WITHDRAWN' WHERE id=?1",
                        params![id],
                    )?;
                    Ok(())
                })
                .await?;
            self.inner
                .create_invoice(amount_sat, memo, expiry_s, external_id)
                .await
        }
        async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
            self.inner.lookup(id).await
        }
        async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
            self.inner.lookup_settlement(id).await
        }
        async fn pay(&self, d: &str, a: u64, k: &str) -> Result<String> {
            self.inner.pay(d, a, k).await
        }
        async fn payment_status(&self, id: &str) -> Result<PayStatus> {
            self.inner.payment_status(id).await
        }
        async fn payment_status_by_key(&self, k: &str) -> Result<PayStatus> {
            self.inner.payment_status_by_key(k).await
        }
        async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
            self.inner.watch().await
        }
    }

    // lnrent-i23 (multi-reviewer pass 4): `listing withdraw` is the operator's stop button, and it
    // must not be outrun by an order already in flight. The §5.4 ACTIVE check runs BEFORE
    // `create_invoice`, a network round-trip to the payment backend, so a withdrawal committed
    // during it would otherwise still see a PENDING subscription and an OPEN invoice committed
    // afterwards — handing a Buyer a payable invoice for a listing that is no longer offered.
    #[tokio::test]
    async fn a_withdrawal_during_the_invoice_mint_refuses_the_order() {
        let store = mem_store();
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(&store, listing_id, "dummy", recipe.pricing.amount_sat as i64).await;
        let payment = Arc::new(WithdrawDuringMint {
            inner: MockPayment::new(),
            store: store.clone(),
            listing_id: listing_id.to_string(),
        });
        // Not the `intake()` helper: it is typed to `Arc<MockPayment>`, and this case needs the
        // wrapper above in the backend slot.
        let handler = OrderIntake::new(
            store.clone(),
            payment,
            Arc::new(TestClock::new(1000)),
            recipe,
            budget_with_room(),
            u32::MAX,
        );

        let out = RecordingOutbound::default();
        handler
            .handle(
                Keys::generate().public_key(),
                order("q", listing_id, json!({})),
                &out,
            )
            .await
            .unwrap();

        let err = expect_order_error(&out);
        assert_eq!(
            err.error.code, "unavailable",
            "the Buyer is told the listing is not being offered, as the §5.4 gate would have"
        );
        // The money path is what this protects: the stop button really stopped it.
        assert_eq!(
            count(&store, "SELECT count(*) FROM subscription").await,
            0,
            "no subscription committed after the withdrawal"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice").await,
            0,
            "no OPEN invoice row committed, so nothing is payable"
        );
        // And the hold is released in the same transaction, not left to lapse on its TTL.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE state IN ('HELD','CONSUMED')"
            )
            .await,
            0,
            "the capacity hold was released with the refusal, not left holding a slot"
        );
    }

    /// A backend that RELEASES this order's capacity hold during the mint, leaving the listing
    /// ACTIVE. That is the state a duplicate delivery of the same `order.request` leaves behind when
    /// its sibling hit the withdrawn-listing branch first and the operator then republished: the
    /// listing reads ACTIVE again, but the shared reservation is already `RELEASED`.
    struct ReleaseHoldDuringMint {
        inner: MockPayment,
        store: Store,
    }

    #[async_trait::async_trait]
    impl PaymentBackend for ReleaseHoldDuringMint {
        async fn create_invoice(
            &self,
            amount_sat: u64,
            memo: &str,
            expiry_s: u32,
            external_id: &str,
        ) -> Result<Invoice> {
            self.store
                .transaction(move |tx| {
                    tx.execute("UPDATE reservation SET state='RELEASED'", [])?;
                    Ok(())
                })
                .await?;
            self.inner
                .create_invoice(amount_sat, memo, expiry_s, external_id)
                .await
        }
        async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
            self.inner.lookup(id).await
        }
        async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
            self.inner.lookup_settlement(id).await
        }
        async fn pay(&self, d: &str, a: u64, k: &str) -> Result<String> {
            self.inner.pay(d, a, k).await
        }
        async fn payment_status(&self, id: &str) -> Result<PayStatus> {
            self.inner.payment_status(id).await
        }
        async fn payment_status_by_key(&self, k: &str) -> Result<PayStatus> {
            self.inner.payment_status_by_key(k).await
        }
        async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
            self.inner.watch().await
        }
    }

    // lnrent-i23: an order must never commit behind a RELEASED hold — the commit tail only refreshes
    // the hold's expiry, never its state, so the slot would read free while a real subscription
    // occupied it and capacity could be handed out twice.
    //
    // The released hold is manufactured with a direct write ON PURPOSE. The duplicate-delivery race
    // this guard was written for is no longer reachable (the refusal is cached in the same
    // transaction that releases, so a later delivery is answered as `Duplicate` first), and a test
    // that pretended otherwise would be narrating a path that cannot happen. What is pinned here is
    // the invariant itself, against whatever future release path might reach this state.
    #[tokio::test]
    async fn an_order_never_commits_behind_a_released_hold() {
        let store = mem_store();
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(&store, listing_id, "dummy", recipe.pricing.amount_sat as i64).await;
        let payment = Arc::new(ReleaseHoldDuringMint {
            inner: MockPayment::new(),
            store: store.clone(),
        });
        let handler = OrderIntake::new(
            store.clone(),
            payment,
            Arc::new(TestClock::new(1000)),
            recipe,
            budget_with_room(),
            u32::MAX,
        );

        let out = RecordingOutbound::default();
        handler
            .handle(
                Keys::generate().public_key(),
                order("q", listing_id, json!({})),
                &out,
            )
            .await
            .unwrap();

        let err = expect_order_error(&out);
        assert_eq!(err.error.code, "unavailable");
        // [9A] non-vacuity: the listing really was ACTIVE the whole time, so this refusal can only
        // have come from the hold check — not from the withdrawn-listing branch beside it.
        assert_eq!(
            listing_state(&store, listing_id).await.as_deref(),
            Some("ACTIVE"),
            "the listing stayed ACTIVE; only the hold was lost"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM subscription").await,
            0,
            "no subscription behind a released hold"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice").await,
            0,
            "and nothing payable was committed"
        );
    }

    // lnrent-i23 (multi-reviewer pass 8): the refusal must be durable in the SAME transaction that
    // releases the capacity hold. `reservation::reserve` deliberately bails on a `RELEASED` row, so
    // if a crash landed between the release and a separately-written cache, every relay redelivery
    // of this request would fail at reserve instead of resending an idempotent `order.error` —
    // stuck until the terminal-row reaper, which is 120d away. Asserting the cached row exists is
    // what proves the two are one durable fact; the send that follows is the resend path.
    #[tokio::test]
    async fn the_withdrawal_refusal_is_cached_with_the_release_not_after_it() {
        let store = mem_store();
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(&store, listing_id, "dummy", recipe.pricing.amount_sat as i64).await;
        let payment = Arc::new(WithdrawDuringMint {
            inner: MockPayment::new(),
            store: store.clone(),
            listing_id: listing_id.to_string(),
        });
        let handler = OrderIntake::new(
            store.clone(),
            payment,
            Arc::new(TestClock::new(1000)),
            recipe,
            budget_with_room(),
            u32::MAX,
        );

        let out = RecordingOutbound::default();
        handler
            .handle(
                Keys::generate().public_key(),
                order("q", listing_id, json!({})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(expect_order_error(&out).error.code, "unavailable");

        // The durable half: an answer exists for this request, and it is the refusal.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM inbound_request WHERE response_msg_type='order.error'"
            )
            .await,
            1,
            "the refusal was cached, so a redelivery resends it instead of failing at reserve"
        );
        // And it is genuinely paired with the release, not written by a later step.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE state='RELEASED'"
            )
            .await,
            1,
            "the hold really was released in that same transaction"
        );
    }

    async fn listing_state(store: &Store, id: &str) -> Option<String> {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT state FROM listing WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()?)
            })
            .await
            .unwrap()
    }

    async fn seed_active_sub(store: &Store, id: &str, buyer_hex: &str, paid_through: i64) {
        let (id, buyer) = (id.to_string(), buyer_hex.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, recipe_id, buyer_pubkey, state, period_s, renew_lead_s, retention_s, paid_through, created_at, updated_at)
                     VALUES (?1, 'dummy', ?2, 'ACTIVE', 2592000, 604800, 604800, ?3, 0, 0)",
                    params![id, buyer, paid_through],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Seed a renewable sub with full control over state, retention, paid_through, and the
    /// downtime-credit FLOOR (`suspend_not_before`), so the credited-window renewal gate (§6.5,
    /// lnrent-7fp.22) can be exercised. period/lead are small fixed values — irrelevant to the gate.
    async fn seed_renewable_sub(
        store: &Store,
        id: &str,
        buyer_hex: &str,
        state: &str,
        paid_through: i64,
        retention_s: i64,
        suspend_not_before: Option<i64>,
    ) {
        let (id, buyer, state) = (id.to_string(), buyer_hex.to_string(), state.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, recipe_id, buyer_pubkey, state, period_s, renew_lead_s, retention_s,
                         paid_through, suspend_not_before, created_at, updated_at)
                     VALUES (?1, 'dummy', ?2, ?3, 100, 10, ?4, ?5, ?6, 0, 0)",
                    params![
                        id,
                        buyer,
                        state,
                        retention_s,
                        paid_through,
                        suspend_not_before
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_cancel_sub(
        store: &Store,
        id: &str,
        buyer_hex: &str,
        state: &str,
        paid_through: Option<i64>,
        next_deadline: Option<i64>,
    ) {
        let (id, buyer, state) = (id.to_string(), buyer_hex.to_string(), state.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, recipe_id, buyer_pubkey, state, period_s, renew_lead_s, retention_s,
                         paid_through, next_deadline, created_at, updated_at)
                     VALUES (?1, 'dummy', ?2, ?3, 100, 10, 500, ?4, ?5, 0, 0)",
                    params![id, buyer, state, paid_through, next_deadline],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_reservation(store: &Store, order_id: &str) {
        let order_id = order_id.to_string();
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO reservation
                        (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                     VALUES (?1, ?2, '{\"cpu\":1}', '{\"count\":0}', 'HELD', 0, 0)",
                    params![format!("res-{order_id}"), order_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Re-stamp a seeded sub's owning recipe (the seeds all write `dummy`), including to NULL for
    /// the lnrent-yjl legacy-row guard.
    async fn set_sub_recipe_id(store: &Store, id: &str, recipe_id: Option<&str>) {
        let (id, recipe_id) = (id.to_string(), recipe_id.map(str::to_string));
        store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE subscription SET recipe_id=?2 WHERE id=?1",
                    params![id, recipe_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Every subscription column a renewal could plausibly move — state, the paid-through anchor,
    /// the downtime-credit floor, the reconcile cursor, and the row's own mtime — so "the refusal
    /// writes nothing durable" is a claim about the ROW, not just about its state.
    async fn sub_snapshot(
        store: &Store,
        id: &str,
    ) -> (String, Option<i64>, Option<i64>, Option<i64>, i64) {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT state, paid_through, suspend_not_before, next_deadline, updated_at
                       FROM subscription WHERE id=?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )?)
            })
            .await
            .unwrap()
    }

    async fn inv_amount_sat(store: &Store, external_id: &str) -> i64 {
        let external_id = external_id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT amount_sat FROM invoice WHERE external_id=?1",
                    params![external_id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    async fn sub_state_and_deadline(store: &Store, id: &str) -> (String, Option<i64>) {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT state, next_deadline FROM subscription WHERE id=?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap()
    }

    async fn outbox_notices(store: &Store, sub_id: &str) -> Vec<(String, String, String)> {
        let sub_id = sub_id.to_string();
        store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, recipient, payload_json
                       FROM outbox
                      WHERE subscription_id=?1 AND msg_type='billing.notice'
                      ORDER BY id",
                )?;
                let rows =
                    stmt.query_map(params![sub_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
                let notices = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(notices)
            })
            .await
            .unwrap()
    }

    const REFUND_DEST: &str = "refunds@example.com";

    fn order(id: &str, listing_id: &str, params: serde_json::Value) -> Msg {
        Msg::OrderRequest(OrderRequest {
            id: id.into(),
            listing_id: listing_id.into(),
            params,
            refund_dest: Some(REFUND_DEST.to_string()),
        })
    }

    fn order_with_refund(id: &str, listing_id: &str, refund_dest: &str) -> Msg {
        Msg::OrderRequest(OrderRequest {
            id: id.into(),
            listing_id: listing_id.into(),
            params: json!({}),
            refund_dest: Some(refund_dest.to_string()),
        })
    }

    fn cancel(sub_id: &str) -> Msg {
        Msg::SubCancel(SubCancel {
            subscription_id: sub_id.into(),
        })
    }

    fn expect_order_error(out: &RecordingOutbound) -> OrderError {
        match out.only().1 {
            Msg::OrderError(e) => e,
            other => panic!("expected order.error, got {other:?}"),
        }
    }

    // Test 1: order.request -> a PENDING subscription + an OPEN invoice (unique external_id) in one
    // transaction, and order.invoice (request_id + order_id + bolt11) is sent.
    #[tokio::test]
    async fn order_request_opens_pending_sub_open_invoice_and_sends_invoice() {
        let store = mem_store();
        let payment = Arc::new(MockPayment::new());
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(
            &store,
            listing_id,
            "dummy",
            recipe.pricing.amount_sat as i64,
        )
        .await;
        let handler = intake(
            store.clone(),
            payment,
            TestClock::new(1000),
            recipe,
            budget_with_room(),
        );

        let sender = Keys::generate().public_key();
        let out = RecordingOutbound::default();
        handler
            .handle(sender, order("req-1", listing_id, json!({})), &out)
            .await
            .unwrap();

        let inv = match out.only().1 {
            Msg::OrderInvoice(i) => i,
            other => panic!("expected order.invoice, got {other:?}"),
        };
        assert_eq!(inv.request_id, "req-1");
        assert!(!inv.order_id.is_empty());
        assert!(!inv.bolt11.is_empty());
        assert_eq!(inv.amount_sat, 100);

        // Exactly one PENDING sub, one OPEN order invoice with the deterministic external_id, and
        // the cached inbound_request row — all written by the single transaction.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM subscription WHERE state='PENDING'"
            )
            .await,
            1
        );
        let want_ext = format!("order:{}:req-1", sender.to_hex());
        assert_eq!(
            count(&store, &format!(
                "SELECT count(*) FROM invoice WHERE status='OPEN' AND kind='order' AND external_id='{want_ext}'"
            )).await,
            1
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM inbound_request").await,
            1
        );
        // The HELD reservation backs the PENDING order.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE state='HELD'"
            )
            .await,
            1
        );
    }

    // Test 2: invalid params / capacity_full / price_changed -> a structured order.error is sent,
    // no dangling PENDING sub or HELD reservation remains, and a pre-order failure carries no
    // order_id.
    #[tokio::test]
    async fn pre_order_failures_send_structured_error_and_leave_no_dangling_state() {
        async fn assert_clean(store: &Store) {
            assert_eq!(
                count(store, "SELECT count(*) FROM subscription").await,
                0,
                "no dangling sub"
            );
            assert_eq!(
                count(store, "SELECT count(*) FROM reservation WHERE state='HELD'").await,
                0,
                "no dangling HELD reservation"
            );
        }

        // params_invalid: wireguard requires a `pubkey` string; empty params fail validation.
        {
            let store = mem_store();
            let handler = intake(
                store.clone(),
                Arc::new(MockPayment::new()),
                TestClock::new(1000),
                wireguard_recipe(),
                budget_with_room(),
            );
            let out = RecordingOutbound::default();
            handler
                .handle(
                    Keys::generate().public_key(),
                    order("p", "30402:op:wg-1", json!({})),
                    &out,
                )
                .await
                .unwrap();
            let err = expect_order_error(&out);
            assert_eq!(err.error.code, "params_invalid");
            assert!(
                err.order_id.is_none(),
                "pre-order failure carries no order_id"
            );
            assert_clean(&store).await;
        }

        // price_changed: the referenced listing is unknown (none seeded).
        {
            let store = mem_store();
            let handler = intake(
                store.clone(),
                Arc::new(MockPayment::new()),
                TestClock::new(1000),
                dummy_recipe(),
                budget_with_room(),
            );
            let out = RecordingOutbound::default();
            handler
                .handle(
                    Keys::generate().public_key(),
                    order("pc", "30402:op:gone", json!({})),
                    &out,
                )
                .await
                .unwrap();
            let err = expect_order_error(&out);
            assert_eq!(err.error.code, "price_changed");
            assert!(err.order_id.is_none());
            assert_clean(&store).await;
        }

        // capacity_full: a recipe needing 1 cpu against a zero-cpu host budget.
        {
            let store = mem_store();
            let mut recipe = dummy_recipe();
            recipe.provisioning.resources.cpu = 1;
            let listing_id = "30402:op:dummy-1";
            seed_listing(
                &store,
                listing_id,
                "dummy",
                recipe.pricing.amount_sat as i64,
            )
            .await;
            let zero_budget = Budget {
                cpu: 0,
                mem_mb: 0,
                disk_gb: 0,
                ports: 0,
            };
            let handler = intake(
                store.clone(),
                Arc::new(MockPayment::new()),
                TestClock::new(1000),
                recipe,
                zero_budget,
            );
            let out = RecordingOutbound::default();
            handler
                .handle(
                    Keys::generate().public_key(),
                    order("cf", listing_id, json!({})),
                    &out,
                )
                .await
                .unwrap();
            let err = expect_order_error(&out);
            assert_eq!(err.error.code, "capacity_full");
            assert!(err.order_id.is_none());
            assert_clean(&store).await;
        }
    }

    // lnrent-i23: the publication gate's safety claim — "a fresh daemon takes no orders until the
    // operator publishes" — is enforced by the `l.state != "ACTIVE"` comparison in the §5.4 price
    // check above and NOWHERE else. Before that bead the comparison was effectively dead (no code
    // path ever wrote a non-ACTIVE state); now UNPUBLISHED is the state every install boots into,
    // so it is the gate. This drives a real `order.request` at both non-ACTIVE states and asserts
    // the refusal, so relaxing that comparison cannot stay green.
    #[tokio::test]
    async fn a_listing_that_is_not_active_takes_no_orders() {
        for state in ["UNPUBLISHED", "WITHDRAWN"] {
            let store = mem_store();
            let recipe = dummy_recipe();
            let listing_id = "30402:op:dummy-1";
            seed_listing_in_state(
                &store,
                listing_id,
                "dummy",
                recipe.pricing.amount_sat as i64,
                state,
            )
            .await;
            let handler = intake(
                store.clone(),
                Arc::new(MockPayment::new()),
                TestClock::new(1000),
                recipe,
                budget_with_room(),
            );

            let out = RecordingOutbound::default();
            handler
                .handle(
                    Keys::generate().public_key(),
                    order("q", listing_id, json!({})),
                    &out,
                )
                .await
                .unwrap();

            let err = expect_order_error(&out);
            // `unavailable`, not `price_changed`: the Operator is not offering this listing, and
            // the price did not move. A Buyer told "price_changed" would re-quote against something
            // that is not for sale.
            assert_eq!(
                err.error.code, "unavailable",
                "a {state} listing is refused as not-offered"
            );
            assert!(err.order_id.is_none(), "no order was created for {state}");
            // The money path is what this protects: no sub, no invoice, no held capacity.
            assert_eq!(
                count(&store, "SELECT count(*) FROM subscription").await,
                0,
                "{state}: no subscription"
            );
            assert_eq!(
                count(&store, "SELECT count(*) FROM invoice").await,
                0,
                "{state}: nothing was minted"
            );
            assert_eq!(
                count(&store, "SELECT count(*) FROM reservation").await,
                0,
                "{state}: no capacity reserved"
            );
        }
    }

    #[tokio::test]
    async fn order_request_rejects_unsafe_request_id_before_derived_rows() {
        let store = mem_store();
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(
            &store,
            listing_id,
            "dummy",
            recipe.pricing.amount_sat as i64,
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            recipe,
            budget_with_room(),
        );

        let sender = Keys::generate().public_key();
        let out = RecordingOutbound::default();
        handler
            .handle(sender, order("x&per_page=1", listing_id, json!({})), &out)
            .await
            .unwrap();
        let err = expect_order_error(&out);
        assert_eq!(err.request_id, "x&per_page=1");
        assert_eq!(err.error.code, "params_invalid");
        assert!(!err.error.retryable);
        assert!(err.order_id.is_none());
        assert_eq!(count(&store, "SELECT count(*) FROM subscription").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM invoice").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM reservation").await, 0);

        let out = RecordingOutbound::default();
        handler
            .handle(sender, order("safe_Req-123", listing_id, json!({})), &out)
            .await
            .unwrap();
        let inv = match out.only().1 {
            Msg::OrderInvoice(i) => i,
            other => panic!("expected order.invoice for safe id, got {other:?}"),
        };
        assert_eq!(inv.request_id, "safe_Req-123");
        assert!(inv.order_id.ends_with(":safe_Req-123"));
        assert_eq!(count(&store, "SELECT count(*) FROM subscription").await, 1);
        assert_eq!(count(&store, "SELECT count(*) FROM reservation").await, 1);
    }

    // Test 3: soft_date or renew.request -> a renewal invoice is issued and billing.invoice is sent.
    #[tokio::test]
    async fn renew_request_and_soft_date_issue_billing_invoice() {
        let store = mem_store();
        let payment = Arc::new(MockPayment::new());
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_active_sub(&store, "sub-1", &buyer_hex, 5000).await;
        let handler = intake(
            store.clone(),
            payment,
            TestClock::new(1000),
            dummy_recipe(),
            budget_with_room(),
        );

        // Buyer renew.request -> billing.invoice correlated by request_id.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-1".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        let (recipient, msg) = out.only();
        assert_eq!(recipient, buyer.public_key());
        let bi = match msg {
            Msg::BillingInvoice(b) => b,
            other => panic!("expected billing.invoice, got {other:?}"),
        };
        assert_eq!(bi.subscription_id, "sub-1");
        assert_eq!(bi.request_id.as_deref(), Some("rr-1"));
        assert!(!bi.bolt11.is_empty());
        assert_eq!(bi.due_at, 5000);
        let req_ext = format!("renew:req:{}:rr-1", buyer.public_key().to_hex());
        assert_eq!(
            count(
                &store,
                &format!(
                    "SELECT count(*) FROM invoice WHERE kind='renewal' AND external_id='{req_ext}'"
                )
            )
            .await,
            1
        );

        // Daemon soft-date auto-renewal -> billing.invoice with NO request_id, sent to the buyer.
        let out2 = RecordingOutbound::default();
        handler
            .issue_soft_date_renewal("sub-1", 5000, &out2)
            .await
            .unwrap();
        let (recipient2, msg2) = out2.only();
        assert_eq!(
            recipient2,
            buyer.public_key(),
            "soft-date invoice goes to the sub's buyer"
        );
        let bi2 = match msg2 {
            Msg::BillingInvoice(b) => b,
            other => panic!("expected billing.invoice, got {other:?}"),
        };
        assert!(
            bi2.request_id.is_none(),
            "an operator-initiated renewal invoice has no request_id"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM invoice WHERE external_id='renew:auto:sub-1:5000'"
            )
            .await,
            1
        );
    }

    // Test 4: a DUPLICATE order.request (same sender+request_id) does NOT create a second
    // order/invoice — it resends the cached response from inbound_request.
    #[tokio::test]
    async fn duplicate_order_request_resends_cached_response_without_second_order() {
        let store = mem_store();
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(
            &store,
            listing_id,
            "dummy",
            recipe.pricing.amount_sat as i64,
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            recipe,
            budget_with_room(),
        );

        let sender = Keys::generate().public_key();
        let out = RecordingOutbound::default();
        handler
            .handle(sender, order("dup", listing_id, json!({})), &out)
            .await
            .unwrap();
        handler
            .handle(sender, order("dup", listing_id, json!({})), &out)
            .await
            .unwrap();

        // Exactly one sub + one invoice despite two identical requests.
        assert_eq!(count(&store, "SELECT count(*) FROM subscription").await, 1);
        assert_eq!(count(&store, "SELECT count(*) FROM invoice").await, 1);

        // Both replies are the identical cached order.invoice.
        let msgs = out.messages();
        assert_eq!(msgs.len(), 2);
        let pick = |m: &Msg| match m {
            Msg::OrderInvoice(i) => (i.order_id.clone(), i.bolt11.clone()),
            other => panic!("expected order.invoice, got {other:?}"),
        };
        assert_eq!(
            pick(&msgs[0].1),
            pick(&msgs[1].1),
            "the duplicate resends the cached order.invoice"
        );
    }

    // P1 (codex pass 1): a renew.request is gated on owner + renewable state — a non-owner cannot
    // mint a billing.invoice for someone else's sub, and a terminal/PENDING sub gets none (capture
    // would only refund such a payment). Both cases drop silently with no reply, no invoice.
    #[tokio::test]
    async fn renew_request_is_gated_on_owner_and_renewable_state() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_active_sub(&store, "sub-1", &buyer.public_key().to_hex(), 5000).await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            dummy_recipe(),
            budget_with_room(),
        );

        // Non-owner renew -> dropped.
        let stranger = Keys::generate();
        let out = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "x".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        assert!(
            out.messages().is_empty(),
            "a non-owner renew is dropped, no reply"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0
        );

        // Owner renew on a now-terminal sub -> dropped.
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET state='TERMINATED' WHERE id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let out2 = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "y".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out2,
            )
            .await
            .unwrap();
        assert!(
            out2.messages().is_empty(),
            "a renew on a terminal sub is dropped"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0
        );
    }

    // lnrent-dvb: a buyer renew.request for a sub ordered under ANOTHER recipe must not be quoted
    // THIS recipe's price. It is refused BEFORE any invoice exists — in the DB or at the backend —
    // and the buyer gets the existing `unavailable` order.error rather than a silent drop, because
    // unlike the daemon-initiated arm (lnrent-6id) this answers a request they made.
    #[tokio::test]
    async fn renew_request_for_a_foreign_recipe_is_refused_before_any_invoice() {
        let store = mem_store();
        let payment = Arc::new(MockPayment::new());
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_active_sub(&store, "sub-1", &buyer_hex, 5000).await;
        // The row belongs to a recipe this daemon does not serve (the `do-vps`-vs-`dummy` shape
        // lnrent-ja2 makes live); the served recipe is priced at a number nothing else here uses.
        set_sub_recipe_id(&store, "sub-1", Some("other-recipe")).await;
        let handler = intake(
            store.clone(),
            payment.clone(),
            TestClock::new(1000),
            priced_recipe(30_000),
            budget_with_room(),
        );
        let before = sub_snapshot(&store, "sub-1").await;

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-foreign".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();

        // The REFUSAL arm fired — not some other reject that happens to error. The message pins this
        // branch: it is the only `unavailable` in the module carrying it, and every other renew
        // reject replies nothing at all.
        let (recipient, msg) = out.only();
        assert_eq!(recipient, buyer.public_key());
        let err = match msg {
            Msg::OrderError(e) => e,
            other => panic!("expected order.error, got {other:?}"),
        };
        assert_eq!(err.request_id, "rr-foreign", "correlated to THIS request");
        assert_eq!(err.order_id, None, "a renewal refusal opens no order");
        assert_eq!(err.error.code, "unavailable");
        assert_eq!(err.error.message, FOREIGN_RECIPE_REFUSAL);
        assert!(
            !err.error.message.contains("other-recipe"),
            "the refusal must not name what else the operator runs"
        );

        // No invoice was minted at the wrong price — in the DB…
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice").await,
            0,
            "no renewal invoice at this recipe's price for a sub ordered under another"
        );
        // …nor at the BACKEND: `settle` errors only when no invoice was ever created for that
        // external_id. A gate placed after the mint would leave a wrong-priced bolt11 live there, and
        // `create_invoice` is idempotent on external_id, so the owning recipe's own renewal would
        // later be handed that stale invoice instead of minting one at its price.
        let ext = format!("renew:req:{buyer_hex}:rr-foreign");
        assert!(
            payment.settle(&ext, 1000).is_err(),
            "no renewal invoice was minted at the backend for a foreign recipe's sub"
        );

        // Nothing durable at all: no cached response claiming the (sender, request_id) key, no outbox
        // DM, no journal entry, and the subscription row — state, paid_through, credit floor, cursor,
        // mtime — is byte-identical.
        assert_eq!(
            count(&store, "SELECT count(*) FROM inbound_request").await,
            0,
            "the refusal is re-derived, never cached"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM event_log").await, 0);
        assert_eq!(
            sub_snapshot(&store, "sub-1").await,
            before,
            "a refused renewal moves no column on the subscription"
        );

        // And it is a REFUSAL, not a consumed request: because the (sender, request_id) key was left
        // unclaimed, the very same id still renews once this daemon is pointed at the recipe that
        // owns the row. A cached refusal would brick that id instead.
        set_sub_recipe_id(&store, "sub-1", Some("dummy")).await;
        let out2 = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-foreign".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out2,
            )
            .await
            .unwrap();
        match out2.only().1 {
            Msg::BillingInvoice(bi) => assert_eq!(bi.amount_sat, 30_000),
            other => panic!("expected billing.invoice once the recipe matches, got {other:?}"),
        }
        assert_eq!(inv_amount_sat(&store, &ext).await, 30_000);
    }

    // lnrent-dvb, the ORDER of the two gates: the recipe gate replies, the owner gate does not, so a
    // stranger probing subscription ids must hit the owner gate FIRST. Otherwise the refusal itself
    // becomes an oracle — "unavailable" for a real id vs silence for an invented one tells any sender
    // which subscription ids exist, which is exactly what §5.1's non-disclosure rule forbids (and
    // what `op.result`'s `unauthorized` posture is written to prevent). Pinned because the two gates
    // are adjacent statements: a reorder would compile, pass every other test, and leak.
    #[tokio::test]
    async fn renew_request_from_a_stranger_is_dropped_silently_even_for_a_foreign_recipe() {
        let store = mem_store();
        let buyer = Keys::generate();
        let stranger = Keys::generate();
        seed_active_sub(&store, "sub-1", &buyer.public_key().to_hex(), 5000).await;
        set_sub_recipe_id(&store, "sub-1", Some("other-recipe")).await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            priced_recipe(30_000),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-probe".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();

        assert!(
            out.messages().is_empty(),
            "a non-owner learns NOTHING — not even that this daemon does not serve the row: {:?}",
            out.messages()
        );
        // …and it is indistinguishable from a subscription id that does not exist at all.
        let out2 = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-probe-2".into(),
                    subscription_id: "no-such-sub".into(),
                }),
                &out2,
            )
            .await
            .unwrap();
        assert!(out2.messages().is_empty());
    }

    // lnrent-dvb, the OTHER gate order: the recipe gate is decided BEFORE the state gate, so an owner
    // gets the refusal whatever state their foreign row is in — including the terminal states whose
    // own reject is a silent drop, and including transient RESUMING, whose z4u notice ("retry in a
    // moment") would be a lie for a row this daemon will never serve. §5.1 and the buyer client's
    // `renew()` doc both assert this ordering; pinned here so neither can go stale by a reorder.
    #[tokio::test]
    async fn renew_request_for_a_foreign_recipe_is_refused_in_every_state() {
        // EVERY §6.3 state, not a sample: a partial list is exactly what let a wrong `retryable`
        // through review, so this pins the whole matrix and a new state must be classified here.
        for state in [
            "PENDING",
            "PROVISIONING",
            "ACTIVE",
            "RESUMING",
            "SUSPENDED",
            "TERMINATED",
            "EXPIRED",
            "CANCELLED",
            "REFUND_DUE",
            "REFUNDED",
        ] {
            let store = mem_store();
            let buyer = Keys::generate();
            seed_active_sub(&store, "sub-1", &buyer.public_key().to_hex(), 5000).await;
            set_sub_recipe_id(&store, "sub-1", Some("other-recipe")).await;
            let s = state.to_string();
            store
                .transaction(move |tx| {
                    tx.execute(
                        "UPDATE subscription SET state=?2 WHERE id=?1",
                        params!["sub-1", s],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            let handler = intake(
                store.clone(),
                Arc::new(MockPayment::new()),
                TestClock::new(1000),
                priced_recipe(30_000),
                budget_with_room(),
            );

            let out = RecordingOutbound::default();
            handler
                .handle(
                    buyer.public_key(),
                    Msg::RenewRequest(RenewRequest {
                        id: "rr-state".into(),
                        subscription_id: "sub-1".into(),
                    }),
                    &out,
                )
                .await
                .unwrap();
            match out.only().1 {
                Msg::OrderError(e) => {
                    assert_eq!(e.error.code, "unavailable", "state {state}");
                    assert_eq!(e.error.message, FOREIGN_RECIPE_REFUSAL, "state {state}");
                    // Whether a repoint could EVER make this renew succeed — not whether it would
                    // succeed now. True for the rows that can still reach ACTIVE/SUSPENDED (PENDING
                    // and PROVISIONING on payment+provision, RESUMING on resume); false for the dead
                    // ends, REFUND_DUE included, since its only exit is REFUNDED.
                    assert_eq!(
                        e.error.retryable,
                        matches!(
                            state,
                            "PENDING" | "PROVISIONING" | "ACTIVE" | "RESUMING" | "SUSPENDED"
                        ),
                        "state {state}: retryable must not promise an impossible retry"
                    );
                }
                other => panic!("state {state}: expected the recipe refusal, got {other:?}"),
            }
            assert_eq!(
                count(&store, "SELECT count(*) FROM invoice").await,
                0,
                "state {state}"
            );
        }
    }

    // lnrent-dvb (the regression that actually matters): a sub whose `recipe_id` MATCHES still
    // renews, and both the minted invoice row and the `billing.invoice` the buyer is quoted carry
    // ITS OWN recipe's price — asserted as a NUMBER, not merely "an invoice appeared".
    #[tokio::test]
    async fn renew_request_is_quoted_the_subscriptions_own_recipe_price() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_active_sub(&store, "sub-1", &buyer_hex, 5000).await; // seeds recipe_id='dummy'
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            priced_recipe(30_000),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-own".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        let bi = match out.only().1 {
            Msg::BillingInvoice(b) => b,
            other => panic!("expected billing.invoice, got {other:?}"),
        };
        assert_eq!(
            bi.amount_sat, 30_000,
            "the buyer is quoted the ordering == serving recipe's price"
        );
        assert_eq!(
            inv_amount_sat(&store, &format!("renew:req:{buyer_hex}:rr-own")).await,
            30_000,
            "and the persisted invoice carries that same amount"
        );
    }

    // lnrent-dvb + the lnrent-yjl regression guard, pinned explicitly: `recipe_id = NULL` is OWNED
    // (`Recipe::owns_row`), so a legacy row — which is every row a single-recipe M1a operator may
    // hold — still renews on request, at the served price. Gating NULL as foreign would refuse those
    // buyers their own renewals: the sub would run out its paid period and lapse with the buyer
    // unable to pay for it, which is exactly the regression yjl shipped and PR #63 reverted.
    #[tokio::test]
    async fn renew_request_still_renews_for_a_legacy_subscription_with_no_recipe_id() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_active_sub(&store, "sub-1", &buyer_hex, 5000).await;
        set_sub_recipe_id(&store, "sub-1", None).await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            priced_recipe(30_000),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-legacy".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        let bi = match out.only().1 {
            Msg::BillingInvoice(b) => b,
            other => panic!("a NULL-owner row keeps the pre-yjl behaviour; got {other:?}"),
        };
        assert_eq!(bi.subscription_id, "sub-1");
        assert_eq!(bi.request_id.as_deref(), Some("rr-legacy"));
        assert_eq!(bi.amount_sat, 30_000);
        assert_eq!(
            inv_amount_sat(&store, &format!("renew:req:{buyer_hex}:rr-legacy")).await,
            30_000
        );
    }

    // mi9.2/DRIFT-3: a malformed renew request id is dropped with NO reply before any invoice or
    // inbound_request row exists — the same 1..=128 / [A-Za-z0-9_-] bound the order path enforces —
    // while a valid id still renews. NO reply because every renew reply correlates by echoing this
    // exact id back, so an unvalidated one has nowhere to go (it is no longer true that the wire
    // has no renew error variant — lnrent-dvb's refusal rides `order.error`).
    #[tokio::test]
    async fn renew_request_with_malformed_id_is_dropped_before_any_row() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_active_sub(&store, "sub-1", &buyer.public_key().to_hex(), 5000).await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            dummy_recipe(),
            budget_with_room(),
        );

        // Empty, over-long, and out-of-alphabet ids — from the OWNER of an ACTIVE sub, so the
        // drop provably comes from the id gate, not the owner/state gates.
        let long = "a".repeat(129);
        for bad in ["", long.as_str(), "sp ace", "semi;colon"] {
            let out = RecordingOutbound::default();
            handler
                .handle(
                    buyer.public_key(),
                    Msg::RenewRequest(RenewRequest {
                        id: bad.into(),
                        subscription_id: "sub-1".into(),
                    }),
                    &out,
                )
                .await
                .unwrap();
            assert!(
                out.messages().is_empty(),
                "a malformed renew id is dropped without a reply"
            );
        }
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM inbound_request").await,
            0
        );

        // A valid id still renews (the gate does not over-drop).
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "ok-1".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(out.messages().len(), 1, "a valid renew id gets an invoice");
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            1
        );
    }

    // lnrent-z4u: a cancel or renew that lands while the sub is transiently RESUMING gets a
    // STATELESS informational BillingNotice (option (a)) — owner-only — and the RESUMING state is
    // NEVER changed. No RESUMING->CANCELLED shortcut, no renewal invoice, no persisted outbox row:
    // the resume driver keeps sole ownership of the state='RESUMING' CAS.
    #[tokio::test]
    async fn cancel_and_renew_during_resuming_notify_owner_without_state_change() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_renewable_sub(&store, "sub-c", &buyer_hex, "RESUMING", 5000, 500, None).await;
        seed_renewable_sub(&store, "sub-r", &buyer_hex, "RESUMING", 5000, 500, None).await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        // Owner cancel while RESUMING -> informational notice, state UNCHANGED.
        let out = RecordingOutbound::default();
        handler
            .handle(buyer.public_key(), cancel("sub-c"), &out)
            .await
            .unwrap();
        match out.only().1 {
            Msg::BillingNotice(n) => {
                assert_eq!(n.subscription_id, "sub-c");
                assert_eq!(n.state, "RESUMING", "the notice reports the live RESUMING state");
            }
            other => panic!("expected a billing.notice, got {other:?}"),
        }
        assert_eq!(
            sub_state_and_deadline(&store, "sub-c").await.0,
            "RESUMING",
            "cancel must not move a RESUMING sub"
        );

        // Owner renew while RESUMING -> the same informational notice, state UNCHANGED.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "r1".into(),
                    subscription_id: "sub-r".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        match out.only().1 {
            Msg::BillingNotice(n) => {
                assert_eq!(n.subscription_id, "sub-r");
                assert_eq!(n.state, "RESUMING");
            }
            other => panic!("expected a billing.notice, got {other:?}"),
        }
        assert_eq!(
            sub_state_and_deadline(&store, "sub-r").await.0,
            "RESUMING",
            "renew must not move a RESUMING sub"
        );

        // The notice is STATELESS and NOTHING else happens: no persisted outbox row, no renewal
        // invoice, no cached inbound_request row, and no RESUMING row flipped to CANCELLED.
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox").await,
            0,
            "the RESUMING notice is a direct reply, not a persisted outbox row"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0,
            "no renewal invoice is minted for a RESUMING sub"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM inbound_request").await,
            0,
            "the stateless notice caches no idempotency row"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM subscription WHERE state='CANCELLED'").await,
            0,
            "no RESUMING row is cancelled"
        );
    }

    // lnrent-z4u: the RESUMING notice is owner-only. A non-owner cancel/renew for a RESUMING sub
    // stays a SILENT drop (no reply) so an outsider never learns the sub exists or its state.
    #[tokio::test]
    async fn cancel_and_renew_during_resuming_from_nonowner_are_silent() {
        let store = mem_store();
        let buyer = Keys::generate();
        let stranger = Keys::generate();
        seed_renewable_sub(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            "RESUMING",
            5000,
            500,
            None,
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(stranger.public_key(), cancel("sub-1"), &out)
            .await
            .unwrap();
        handler
            .handle(
                stranger.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "r1".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();

        assert!(
            out.messages().is_empty(),
            "a non-owner never gets the RESUMING notice"
        );
        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await.0,
            "RESUMING",
            "a non-owner request never changes state"
        );
    }

    #[tokio::test]
    async fn cancel_owner_active_marks_cancelled_notifies_and_journals() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_cancel_sub(
            &store,
            "sub-1",
            &buyer_hex,
            "ACTIVE",
            Some(5000),
            Some(4400),
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(buyer.public_key(), cancel("sub-1"), &out)
            .await
            .unwrap();

        assert!(
            out.messages().is_empty(),
            "sub.cancel is fire-and-forget, no direct reply"
        );
        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await,
            ("CANCELLED".to_string(), Some(5000)),
            "ACTIVE cancel terminates at paid_through, not a retention deadline"
        );
        let notices = outbox_notices(&store, "sub-1").await;
        assert_eq!(notices.len(), 1, "exactly one cancel billing.notice");
        assert_eq!(notices[0].0, "outbox:cancel-notice:sub-1:5000");
        assert_eq!(notices[0].1, buyer_hex);
        let notice = match serde_json::from_str::<Msg>(&notices[0].2).unwrap() {
            Msg::BillingNotice(n) => n,
            other => panic!("expected billing.notice payload, got {other:?}"),
        };
        assert_eq!(notice.subscription_id, "sub-1");
        assert_eq!(notice.state, "CANCELLED");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='order_intake_cancel' AND subscription_id='sub-1'"
            )
            .await,
            1,
            "cancel is journaled"
        );
    }

    #[tokio::test]
    async fn cancel_owner_suspended_keeps_existing_deadline() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_cancel_sub(
            &store,
            "sub-1",
            &buyer_hex,
            "SUSPENDED",
            Some(1000),
            Some(1500),
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        handler
            .handle(
                buyer.public_key(),
                cancel("sub-1"),
                &RecordingOutbound::default(),
            )
            .await
            .unwrap();

        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await,
            ("CANCELLED".to_string(), Some(1500)),
            "SUSPENDED cancel keeps the retention deadline already on next_deadline"
        );
        assert_eq!(outbox_notices(&store, "sub-1").await.len(), 1);
    }

    #[tokio::test]
    async fn cancel_nonowner_is_silent_noop() {
        let store = mem_store();
        let buyer = Keys::generate();
        let stranger = Keys::generate();
        seed_cancel_sub(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            "ACTIVE",
            Some(5000),
            Some(4400),
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(stranger.public_key(), cancel("sub-1"), &out)
            .await
            .unwrap();

        assert!(out.messages().is_empty());
        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await,
            ("ACTIVE".to_string(), Some(4400))
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox").await,
            0,
            "non-owner cancel enqueues nothing"
        );
    }

    #[tokio::test]
    async fn cancel_terminal_and_non_cancellable_states_are_noops() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        let states = [
            "TERMINATED",
            "CANCELLED",
            "REFUND_DUE",
            "EXPIRED",
            "REFUNDED",
            "PENDING",
            "PROVISIONING",
        ];
        for state in states {
            let sub_id = format!("sub-{state}");
            seed_cancel_sub(&store, &sub_id, &buyer_hex, state, Some(1000), Some(1500)).await;
        }
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        for state in states {
            handler
                .handle(buyer.public_key(), cancel(&format!("sub-{state}")), &out)
                .await
                .unwrap();
            assert_eq!(
                sub_state_and_deadline(&store, &format!("sub-{state}")).await,
                (state.to_string(), Some(1500)),
                "{state} cancel is a no-op"
            );
        }
        assert!(out.messages().is_empty());
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='order_intake_cancel'"
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn cancel_duplicate_enqueues_one_notice_and_one_journal() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_cancel_sub(
            &store,
            "sub-1",
            &buyer_hex,
            "ACTIVE",
            Some(5000),
            Some(4400),
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(buyer.public_key(), cancel("sub-1"), &out)
            .await
            .unwrap();
        handler
            .handle(buyer.public_key(), cancel("sub-1"), &out)
            .await
            .unwrap();

        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await,
            ("CANCELLED".to_string(), Some(5000))
        );
        assert_eq!(outbox_notices(&store, "sub-1").await.len(), 1);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='order_intake_cancel'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn cancel_with_null_term_deadline_is_noop() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_cancel_sub(
            &store,
            "active-null",
            &buyer_hex,
            "ACTIVE",
            None,
            Some(4400),
        )
        .await;
        seed_cancel_sub(
            &store,
            "suspended-null",
            &buyer_hex,
            "SUSPENDED",
            Some(1000),
            None,
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );

        let out = RecordingOutbound::default();
        handler
            .handle(buyer.public_key(), cancel("active-null"), &out)
            .await
            .unwrap();
        handler
            .handle(buyer.public_key(), cancel("suspended-null"), &out)
            .await
            .unwrap();

        assert_eq!(
            sub_state_and_deadline(&store, "active-null").await,
            ("ACTIVE".to_string(), Some(4400))
        );
        assert_eq!(
            sub_state_and_deadline(&store, "suspended-null").await,
            ("SUSPENDED".to_string(), None)
        );
        assert!(out.messages().is_empty());
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='order_intake_cancel'"
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn cancelled_active_sub_terminates_on_reconcile_deadline() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_cancel_sub(&store, "sub-1", &buyer_hex, "ACTIVE", Some(1500), Some(900)).await;
        seed_reservation(&store, "sub-1").await;
        let (recipe, _suspend_marker, destroy_marker) = marker_recipe();
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            recipe.clone(),
            budget_with_room(),
        );

        handler
            .handle(
                buyer.public_key(),
                cancel("sub-1"),
                &RecordingOutbound::default(),
            )
            .await
            .unwrap();
        let reconciler =
            crate::reconcile::Reconciler::new(store.clone(), Arc::new(MockPayment::new()), recipe);
        let report = reconciler.reconcile_tick(1500).await.unwrap();

        assert_eq!(report.terminated, 1);
        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await,
            ("TERMINATED".to_string(), None)
        );
        assert!(destroy_marker.exists(), "destroy hook ran");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE order_id='sub-1' AND state='RELEASED'"
            )
            .await,
            1,
            "reservation is released in the terminate txn"
        );
    }

    #[tokio::test]
    async fn cancel_stops_new_renewal_and_late_renewal_settlement_refunds() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_cancel_sub(
            &store,
            "sub-1",
            &buyer_hex,
            "ACTIVE",
            Some(5000),
            Some(4400),
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1234),
            dummy_recipe(),
            budget_with_room(),
        );
        handler
            .handle(
                buyer.public_key(),
                cancel("sub-1"),
                &RecordingOutbound::default(),
            )
            .await
            .unwrap();

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-after-cancel".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        assert!(
            out.messages().is_empty(),
            "renew.request after cancel is dropped"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0,
            "no new renewal invoice after cancel"
        );

        let ext = "renew:auto:sub-1:5000";
        seed_open_renewal_invoice(&store, ext, "sub-1").await;
        let outcome = crate::capture::capture(
            &store,
            crate::backends::Settlement {
                invoice_id: format!("inv-{ext}"),
                external_id: ext.to_string(),
                amount_sat: dummy_recipe().pricing.amount_sat,
                received_msat: dummy_recipe().pricing.amount_sat.saturating_mul(1000),
                settled_at: 1234,
            },
            1234,
        )
        .await
        .unwrap();
        assert_eq!(outcome, crate::capture::Capture::RefundDue);
        assert_eq!(
            count(&store, "SELECT count(*) FROM refund_attempt").await,
            1,
            "a renewal settlement on CANCELLED is refunded"
        );
        assert_eq!(
            sub_state_and_deadline(&store, "sub-1").await,
            ("CANCELLED".to_string(), Some(5000)),
            "refund path does not resurrect the sub"
        );
    }

    // P1 (codex pass 1): an unpaid PENDING order's sub carries next_deadline = the invoice expiry,
    // so the reconcile cursor (next_deadline <= now, lnrent-7fp.9) can expire it before a late
    // settlement is captured. A NULL next_deadline would make the order invisible to reconcile.
    #[tokio::test]
    async fn pending_order_sets_next_deadline_to_invoice_expiry() {
        let store = mem_store();
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(
            &store,
            listing_id,
            "dummy",
            recipe.pricing.amount_sat as i64,
        )
        .await;
        let handler = intake(
            store.clone(),
            Arc::new(MockPayment::new()),
            TestClock::new(1000),
            recipe,
            budget_with_room(),
        );
        let sender = Keys::generate().public_key();
        let out = RecordingOutbound::default();
        handler
            .handle(sender, order("nd-1", listing_id, json!({})), &out)
            .await
            .unwrap();

        let (next_deadline, expires_at): (Option<i64>, i64) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT s.next_deadline, i.expires_at FROM subscription s
                     JOIN invoice i ON i.subscription_id = s.id WHERE s.state='PENDING'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(
            next_deadline,
            Some(expires_at),
            "PENDING order next_deadline must equal the invoice expiry so reconcile can expire it"
        );
    }

    // lnrent-7fp.22 FIX A: a buyer renew.request INSIDE the credited resumable window
    // (paid_through + retention_s <= now < B = max(paid_through, suspend_not_before) + retention_s)
    // is ACCEPTED — a downtime credit keeps the sub resumable past the raw retention boundary, so the
    // gate must honor the credited boundary, not raw paid_through. And it is consistent with capture:
    // a settlement at the SAME now RESUMES the sub (it does not refund).
    #[tokio::test]
    async fn renew_request_in_credited_window_is_accepted_and_capture_resumes() {
        let store = mem_store();
        let payment = Arc::new(MockPayment::new());
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        // paid_through=1000, retention=500 -> raw boundary 1500. Credited floor 6000 ->
        // effective_suspend_at = max(1000, 6000) = 6000 -> credited boundary B = 6500. The sub is
        // still in its credited resumable window; now=2200 is in [1500, 6500): past the RAW boundary,
        // before the CREDITED one. B is also more than the default 1h invoice expiry away, so the
        // default expiry is used.
        seed_renewable_sub(
            &store,
            "sub-1",
            &buyer_hex,
            "SUSPENDED",
            1000,
            500,
            Some(6000),
        )
        .await;
        let now = 2200;
        payment.set_now(now); // so the issued invoice's absolute expiry is sane (now + expiry_s)
        let handler = intake(
            store.clone(),
            payment.clone(),
            TestClock::new(now),
            dummy_recipe(),
            budget_with_room(),
        );

        // Accepted: a billing.invoice is issued (raw gate would have DROPPED this with no reply).
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-credit".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        let (_, msg) = out.only();
        let bi = match msg {
            Msg::BillingInvoice(b) => b,
            other => panic!("expected billing.invoice (renewal accepted), got {other:?}"),
        };
        assert_eq!(bi.subscription_id, "sub-1");
        assert_eq!(
            bi.due_at, 1000,
            "due_at stays anchored to paid_through, never the credited floor"
        );
        assert_eq!(
            bi.expires_at,
            now + i64::from(INVOICE_EXPIRY_S),
            "credited-window renewal invoice should keep the default expiry while it is before B"
        );
        let ext = format!("renew:req:{buyer_hex}:rr-credit");
        assert_eq!(
            count(
                &store,
                &format!(
                    "SELECT count(*) FROM invoice WHERE kind='renewal' AND external_id='{ext}'"
                )
            )
            .await,
            1,
            "the credited-window renewal invoice was issued"
        );

        // Consistency with capture: a settlement of that very invoice at the SAME now RESUMES the
        // sub (extends paid_through, ACTIVE) — it does not refund. Issuance and capture agree on B.
        let settlement = crate::backends::Settlement {
            invoice_id: format!("inv-{ext}"),
            external_id: ext.clone(),
            amount_sat: dummy_recipe().pricing.amount_sat,
            received_msat: dummy_recipe().pricing.amount_sat.saturating_mul(1000),
            settled_at: now,
        };
        let outcome = crate::capture::capture(&store, settlement, now)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            crate::capture::Capture::Resumed,
            "capture resumes a settlement inside the credited window — consistent with the accepted renew"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM refund_attempt").await,
            0,
            "no refund for a settlement inside the credited window"
        );
    }

    #[tokio::test]
    async fn renew_request_caps_invoice_expiry_to_resumable_window_and_refuses_tiny_window() {
        async fn issue_with_remaining(remaining: i64) -> (Store, RecordingOutbound, i64) {
            let store = mem_store();
            let payment = Arc::new(MockPayment::new());
            let buyer = Keys::generate();
            let buyer_hex = buyer.public_key().to_hex();
            let paid_through = 1000;
            let suspend_not_before = 10_000;
            let retention_s = 500;
            let resumable_until = suspend_not_before + retention_s;
            let now = resumable_until - remaining;
            seed_renewable_sub(
                &store,
                "sub-1",
                &buyer_hex,
                "SUSPENDED",
                paid_through,
                retention_s,
                Some(suspend_not_before),
            )
            .await;
            payment.set_now(now);
            let handler = intake(
                store.clone(),
                payment,
                TestClock::new(now),
                dummy_recipe(),
                budget_with_room(),
            );
            let out = RecordingOutbound::default();
            handler
                .handle(
                    buyer.public_key(),
                    Msg::RenewRequest(RenewRequest {
                        id: format!("rr-{remaining}"),
                        subscription_id: "sub-1".into(),
                    }),
                    &out,
                )
                .await
                .unwrap();
            (store, out, resumable_until)
        }

        let (store, out, resumable_until) = issue_with_remaining(120).await;
        let (_, msg) = out.only();
        let bi = match msg {
            Msg::BillingInvoice(b) => b,
            other => panic!("expected billing.invoice for short remaining window, got {other:?}"),
        };
        assert!(
            bi.expires_at <= resumable_until,
            "renewal invoice expires after resumable boundary: expires_at={}, B={}",
            bi.expires_at,
            resumable_until
        );
        assert_eq!(bi.expires_at, resumable_until);
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            1
        );

        let (store, out, resumable_until) =
            issue_with_remaining(MIN_RENEWAL_INVOICE_EXPIRY_S).await;
        let (_, msg) = out.only();
        let bi = match msg {
            Msg::BillingInvoice(b) => b,
            other => panic!("expected billing.invoice at exact floor, got {other:?}"),
        };
        assert_eq!(bi.expires_at, resumable_until);
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            1,
            "exact-floor remaining time is still issued"
        );

        let (store, out, _) = issue_with_remaining(MIN_RENEWAL_INVOICE_EXPIRY_S - 1).await;
        assert!(
            out.messages().is_empty(),
            "below-floor renewal should be dropped with no reply"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0,
            "below-floor renewal should not issue an invoice"
        );
    }

    // lnrent-7fp.22 FIX A: a buyer renew.request AT/AFTER the credited boundary B is past the
    // (credited) window — dropped silently, no reply, no invoice — and capture is consistent: a
    // settlement at the SAME now is terminal and REFUNDS.
    #[tokio::test]
    async fn renew_request_past_credited_window_is_dropped_and_capture_refunds() {
        let store = mem_store();
        let payment = Arc::new(MockPayment::new());
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        // Same shape: credited boundary B = 2000 + 500 = 2500. now = 2500 is AT B (inclusive-terminal).
        seed_renewable_sub(
            &store,
            "sub-1",
            &buyer_hex,
            "SUSPENDED",
            1000,
            500,
            Some(2000),
        )
        .await;
        let now = 2500;
        payment.set_now(now);
        let handler = intake(
            store.clone(),
            payment.clone(),
            TestClock::new(now),
            dummy_recipe(),
            budget_with_room(),
        );

        // Dropped: no reply, no renewal invoice.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                Msg::RenewRequest(RenewRequest {
                    id: "rr-late".into(),
                    subscription_id: "sub-1".into(),
                }),
                &out,
            )
            .await
            .unwrap();
        assert!(
            out.messages().is_empty(),
            "a renew at/after the credited boundary is dropped, no reply"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0,
            "no renewal invoice past the credited window"
        );

        // Consistency with capture: had such a payment somehow arrived (e.g. a stale invoice), a
        // settlement at the SAME now is terminal -> RefundDue. Both gates agree the window has closed.
        let ext = "renew:auto:sub-1:1000";
        seed_open_renewal_invoice(&store, ext, "sub-1").await;
        let settlement = crate::backends::Settlement {
            invoice_id: format!("inv-{ext}"),
            external_id: ext.to_string(),
            amount_sat: dummy_recipe().pricing.amount_sat,
            received_msat: dummy_recipe().pricing.amount_sat.saturating_mul(1000),
            settled_at: now,
        };
        let outcome = crate::capture::capture(&store, settlement, now)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            crate::capture::Capture::RefundDue,
            "capture refunds a settlement at/after the credited boundary — consistent with the dropped renew"
        );
    }

    // lnrent-ug8/F3+F6: every new payable order must carry a re-resolvable refund_dest at intake,
    // BEFORE params/reservation/invoice/sub writes. Missing, malformed, BOLT12, and raw BOLT11 are
    // rejected with a structured `refund_dest_invalid` and leave no dangling state; a supported
    // LN-address commits normally.
    #[tokio::test]
    async fn order_time_requires_reresolvable_refund_dest() {
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";

        async fn seeded_handler(recipe: Recipe, listing_id: &str) -> (Store, OrderIntake) {
            let store = mem_store();
            seed_listing(
                &store,
                listing_id,
                "dummy",
                recipe.pricing.amount_sat as i64,
            )
            .await;
            let handler = intake(
                store.clone(),
                Arc::new(MockPayment::new()),
                TestClock::new(1000),
                recipe,
                budget_with_room(),
            );
            (store, handler)
        }

        async fn assert_rejected(recipe: Recipe, listing_id: &str, msg: Msg, want_code: &str) {
            let (store, handler) = seeded_handler(recipe, listing_id).await;
            let out = RecordingOutbound::default();
            handler
                .handle(Keys::generate().public_key(), msg, &out)
                .await
                .unwrap();
            let err = expect_order_error(&out);
            assert_eq!(err.error.code, want_code);
            assert!(!err.error.retryable);
            assert!(
                err.order_id.is_none(),
                "a refund-dest failure carries no order_id"
            );
            assert_eq!(count(&store, "SELECT count(*) FROM subscription").await, 0);
            assert_eq!(count(&store, "SELECT count(*) FROM invoice").await, 0);
            assert_eq!(
                count(
                    &store,
                    "SELECT count(*) FROM reservation WHERE state='HELD'"
                )
                .await,
                0,
                "no reservation is held for a rejected order"
            );
        }

        // Missing refund_dest -> rejected before invoice/reservation/subscription writes.
        assert_rejected(
            recipe.clone(),
            listing_id,
            Msg::OrderRequest(OrderRequest {
                id: "rd-missing".into(),
                listing_id: listing_id.into(),
                params: json!({}),
                refund_dest: None,
            }),
            "refund_dest_invalid",
        )
        .await;

        // Empty refund_dest is equivalent to missing.
        assert_rejected(
            recipe.clone(),
            listing_id,
            order_with_refund("rd-empty", listing_id, "  "),
            "refund_dest_invalid",
        )
        .await;

        // Raw BOLT11 -> rejected: durable orders require a re-resolvable route.
        let bolt11 = crate::refund_resolver::mint_bolt11(
            1_000,
            r#"[["text/plain","lnrent refund"]]"#,
            1_000,
            3_600,
        );
        assert_rejected(
            recipe.clone(),
            listing_id,
            order_with_refund("rd-bolt11", listing_id, &bolt11),
            "refund_dest_invalid",
        )
        .await;

        // BOLT12 offer -> rejected, no dangling sub/reservation, no order_id on the error.
        assert_rejected(
            recipe.clone(),
            listing_id,
            order_with_refund("rd-bolt12", listing_id, "lno1pqps7sjqpgz"),
            "refund_dest_invalid",
        )
        .await;

        // An `lnurl1` decoding to a non-HTTPS URL -> rejected up front (it would only park the refund
        // FAILED at resolve time otherwise, review P2). Proves the order path runs the stricter
        // `validate_dest_format`, not the bare bech32-decoding `detect_form`.
        {
            let http_lnurl = bech32::encode::<bech32::Bech32>(
                bech32::Hrp::parse("lnurl").unwrap(),
                "http://example.com/lnurlp/u".as_bytes(),
            )
            .unwrap();
            assert_rejected(
                recipe.clone(),
                listing_id,
                order_with_refund("rd-lnurl-http", listing_id, &http_lnurl),
                "refund_dest_invalid",
            )
            .await;
        }

        // A supported Lightning address -> the order commits to a PENDING sub and OPEN invoice.
        {
            let (store, handler) = seeded_handler(recipe.clone(), listing_id).await;
            let out = RecordingOutbound::default();
            handler
                .handle(
                    Keys::generate().public_key(),
                    order_with_refund("rd-addr", listing_id, "alice@example.com"),
                    &out,
                )
                .await
                .unwrap();
            assert!(
                matches!(out.only().1, Msg::OrderInvoice(_)),
                "a valid refund_dest commits the order"
            );
            assert_eq!(
                count(
                    &store,
                    "SELECT count(*) FROM invoice WHERE kind='order' AND status='OPEN'"
                )
                .await,
                1,
                "a valid refund_dest mints the order invoice"
            );
            let refund_dest: Option<String> = store
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT refund_dest FROM subscription WHERE state='PENDING'",
                        [],
                        |r| r.get(0),
                    )?)
                })
                .await
                .unwrap();
            assert_eq!(refund_dest.as_deref(), Some("alice@example.com"));
        }
    }

    /// Seed a standalone OPEN renewal invoice (no daemon issuance), so a capture-consistency check
    /// has an invoice to settle against the credited-window boundary.
    async fn seed_open_renewal_invoice(store: &Store, external_id: &str, sub_id: &str) {
        let (ext, sub) = (external_id.to_string(), sub_id.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                     VALUES (?1, ?2, ?3, 'renewal', 100, 'OPEN', 0)",
                    params![format!("inv-{ext}"), sub, ext],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    // PR-1 integration: the per-pubkey cap surfaces through order intake as
    // order.error{capacity_full} for the (cap+1)th order from one buyer key, while a different buyer
    // key still orders. Drives the real OrderIntake -> reserve() plumbing end to end (the cap is
    // threaded config -> OrderIntake -> reserve). The dummy recipe reserves zero resources, so the
    // per-pubkey CAP, not the host budget, is the only limiter here.
    #[tokio::test]
    async fn per_buyer_hold_cap_surfaces_capacity_full_through_order_intake() {
        let store = mem_store();
        let payment = Arc::new(MockPayment::new());
        let recipe = dummy_recipe();
        let listing_id = "30402:op:dummy-1";
        seed_listing(
            &store,
            listing_id,
            "dummy",
            recipe.pricing.amount_sat as i64,
        )
        .await;
        // cap = 1 live hold per buyer key.
        let handler = OrderIntake::new(
            store.clone(),
            payment,
            Arc::new(TestClock::new(1000)),
            recipe,
            budget_with_room(),
            1,
        );

        let buyer_a = Keys::generate().public_key();
        // A's first order -> order.invoice (reserved).
        let out1 = RecordingOutbound::default();
        handler
            .handle(buyer_a, order("a-1", listing_id, json!({})), &out1)
            .await
            .unwrap();
        assert!(
            matches!(out1.only().1, Msg::OrderInvoice(_)),
            "A's first order is invoiced"
        );

        // A's second DISTINCT order -> order.error{capacity_full} (A is at the cap of 1).
        let out2 = RecordingOutbound::default();
        handler
            .handle(buyer_a, order("a-2", listing_id, json!({})), &out2)
            .await
            .unwrap();
        assert_eq!(
            expect_order_error(&out2).error.code,
            "capacity_full",
            "A's order over the per-buyer cap is refused capacity_full"
        );

        // A DIFFERENT buyer key still orders freely (per-pubkey, not global).
        let buyer_b = Keys::generate().public_key();
        let out3 = RecordingOutbound::default();
        handler
            .handle(buyer_b, order("b-1", listing_id, json!({})), &out3)
            .await
            .unwrap();
        assert!(
            matches!(out3.only().1, Msg::OrderInvoice(_)),
            "a second buyer key is unaffected by A's cap"
        );
    }
}
