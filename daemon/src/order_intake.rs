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

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};

use lnrent_wire::{
    BillingInvoice, Msg, OrderError, OrderInvoice, OrderRequest, PublicKey, RenewRequest, WireError,
};

use crate::backends::PaymentBackend;
use crate::clock::Clock;
use crate::nostr_engine::{OrderHandler, Outbound};
use crate::recipe::Recipe;
use crate::reservation::{self, Budget, Request, Reserve};
use crate::store::Store;

/// Lightning expiry stamped on a first-order / renewal invoice (seconds). The order's capacity
/// reservation is held until this same horizon, then released (§9.3). An internal default, not an
/// operator config knob (scope: lnrent-7fp.17).
const INVOICE_EXPIRY_S: u32 = 3600;

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
    ) -> Self {
        Self {
            store,
            payment,
            clock,
            recipe,
            budget,
        }
    }

    /// The `order.request` flow (SPEC.md §6.6 ordering): dedup → validate → reserve → invoice →
    /// one-transaction write → send after commit.
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

        let now = self.clock.now();
        let sender_hex = sender.to_hex();
        let order_id = format!("ord:{sender_hex}:{}", req.id);

        // 2a. VALIDATE params against the recipe (§7.1). A pre-order failure carries NO order_id.
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

        // 2b. PRICE check: the referenced listing must still be the current, ACTIVE one for this
        //     recipe at the published price — a stale/unknown price is `price_changed` (§5.4).
        let listing = self.load_listing(&req.listing_id).await?;
        let stale = match &listing {
            None => true,
            Some(l) => {
                l.state != "ACTIVE"
                    || l.recipe_id.as_deref() != Some(self.recipe.service.id.as_str())
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
        let invoice = match self.payment.create_invoice(
            amount_sat,
            &format!("lnrent order {order_id}"),
            INVOICE_EXPIRY_S,
            &external_id,
        ) {
            Ok(inv) => inv,
            Err(e) => {
                // No sub committed yet — release the HELD reservation, then a structured error.
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        Some(&order_id),
                        unavailable(&format!("payment backend unavailable: {e}")),
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
        let owned = OrderWrite {
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
            Ok(w) => w,
            Err(e) => {
                return self
                    .fail_order(
                        &sender,
                        &req.id,
                        Some(&order_id),
                        unavailable(&format!("store write failed: {e}")),
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
        let now = self.clock.now();
        // Authorize + gate state: only the OWNING buyer may renew, and only a renewable
        // (ACTIVE/SUSPENDED) subscription. Otherwise drop silently — an outsider must not be able
        // to mint a payable billing.invoice for someone else's sub, and a PENDING/terminal sub must
        // not get a renewal invoice that capture would later refund (§5.1 sender auth, §6.3).
        let Some((buyer_hex, state, paid_through, retention_s, suspend_not_before)) =
            self.load_renewable(&req.subscription_id).await?
        else {
            tracing::warn!(sub = %req.subscription_id, "renew.request for unknown subscription — dropped");
            return Ok(());
        };
        if buyer_hex != sender.to_hex() {
            tracing::warn!(sub = %req.subscription_id, "renew.request from a non-owner — dropped");
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
            invoice_expiry_s =
                u32::try_from((resumable_until - now).max(i64::from(INVOICE_EXPIRY_S)))
                    .unwrap_or(u32::MAX);
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

    /// Issue the daemon soft-date auto-renewal invoice for `subscription_id` (no buyer request),
    /// where `cycle_anchor` is the `paid_through` being renewed — so one cycle yields one invoice
    /// via the deterministic `renew:auto:<sub>:<cycle_anchor>` external_id (§6.6). Sends
    /// `billing.invoice` with no `request_id` to the subscription's buyer. This is the issuance
    /// seam the soft-date deadline firing (lnrent-7fp.9) invokes; this bead does not fire it.
    pub async fn issue_soft_date_renewal(
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

    /// The fields a buyer renewal must be authorized against: `(buyer_pubkey_hex, state,
    /// paid_through, retention_s, suspend_not_before)` if the subscription exists, else `None`.
    /// `suspend_not_before` is the downtime-credit FLOOR (§6.5); it widens the renewal eligibility
    /// window the same way it widens capture's resumable boundary.
    async fn load_renewable(
        &self,
        sub_id: &str,
    ) -> Result<Option<(String, String, Option<i64>, i64, Option<i64>)>> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT buyer_pubkey, state, paid_through, retention_s, suspend_not_before
                     FROM subscription WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok((
                            r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            r.get::<_, Option<i64>>(2)?,
                            r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                            r.get::<_, Option<i64>>(4)?,
                        ))
                    },
                )
                .optional()?)
            })
            .await
    }

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
        let hex = hex.ok_or_else(|| anyhow!("subscription {sub_id} has no buyer to renew for"))?;
        PublicKey::from_hex(&hex).context("parsing subscription buyer pubkey")
    }
}

#[async_trait]
impl OrderHandler for OrderIntake {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()> {
        match msg {
            Msg::OrderRequest(req) => self.handle_order(sender, req, out).await,
            Msg::RenewRequest(req) => self.handle_renew(sender, req, out).await,
            // sub.cancel and delivery.resend.request are routed here by the engine but owned by
            // other beads (cancellation; provisioning redelivery, lnrent-7fp.10) — out of scope.
            _ => Ok(()),
        }
    }
}

/// Owned inputs for the atomic order write, so the transaction closure is `move + 'static`.
struct OrderWrite {
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
    fn write(self, tx: &rusqlite::Transaction) -> Result<Option<String>> {
        let existing: Option<String> = tx
            .query_row(
                "SELECT response_json FROM inbound_request WHERE sender_pubkey=?1 AND request_id=?2",
                params![self.sender_hex, self.request_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(json) = existing {
            return Ok(Some(json));
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
        Ok(None)
    }
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

// The five `order.error` codes (§5.1) — the only ones this handler emits. `retryable` follows the
// nature of the failure: a bad request is permanent, capacity / backend / store trouble is not.
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
    use crate::store::{Store, SCHEMA};
    use std::sync::Mutex;

    use crate::backends::MockPayment;
    use lnrent_wire::Keys;
    use nostr::EventId;
    use rusqlite::Connection;
    use serde_json::json;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
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
        OrderIntake::new(store, payment, Arc::new(clock), recipe, budget)
    }

    async fn seed_listing(store: &Store, id: &str, recipe_id: &str, amount_sat: i64) {
        let (id, recipe_id) = (id.to_string(), recipe_id.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO listing
                        (id, recipe_id, d_tag, amount_sat, period_s, renew_lead_s, retention_s, state, updated_at)
                     VALUES (?1, ?2, 'd', ?3, 2592000, 604800, 604800, 'ACTIVE', 0)",
                    params![id, recipe_id, amount_sat],
                )?;
                Ok(())
            })
            .await
            .unwrap();
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

    async fn count(store: &Store, sql: &str) -> i64 {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    fn order(id: &str, listing_id: &str, params: serde_json::Value) -> Msg {
        Msg::OrderRequest(OrderRequest {
            id: id.into(),
            listing_id: listing_id.into(),
            params,
            refund_dest: None,
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
        // before the CREDITED one. B is also more than the default 1h invoice expiry away.
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
        assert!(
            bi.expires_at >= 6500,
            "credited-window renewal invoice expires at {}, before B=6500",
            bi.expires_at
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
}
