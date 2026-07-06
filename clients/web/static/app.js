(() => {
  const state = {
    WebBuyer: null,
    bolt11QrSvg: null,
    buyer: null,
    requestedSignerMode: "auto",
    resolvedSignerMode: null,
    capabilities: { nostr: false, nip44: false, webln: false },
    listings: [],
    selectedListing: null,
    invoice: null,
    subscriptionId: null,
    provision: null,
    waiting: false,
  };

  const el = {
    capNostr: document.querySelector("#cap-nostr"),
    capWebln: document.querySelector("#cap-webln"),
    configForm: document.querySelector("#config-form"),
    relayUrl: document.querySelector("#relay-url"),
    operatorNpub: document.querySelector("#operator-npub"),
    signerMode: document.querySelector("#signer-mode"),
    status: document.querySelector("#status"),
    error: document.querySelector("#error"),
    signerPanel: document.querySelector("#signer-panel"),
    listingsSection: document.querySelector("#listings-section"),
    listingCount: document.querySelector("#listing-count"),
    listings: document.querySelector("#listings"),
    refreshListings: document.querySelector("#refresh-listings"),
    orderSection: document.querySelector("#order-section"),
    orderTitle: document.querySelector("#order-title"),
    clearOrder: document.querySelector("#clear-order"),
    orderForm: document.querySelector("#order-form"),
    paramFields: document.querySelector("#param-fields"),
    refundDest: document.querySelector("#refund-dest"),
    invoiceSection: document.querySelector("#invoice-section"),
    invoiceSummary: document.querySelector("#invoice-summary"),
    invoiceMode: document.querySelector("#invoice-mode"),
    invoiceMeta: document.querySelector("#invoice-meta"),
    invoiceActions: document.querySelector("#invoice-actions"),
    qrBox: document.querySelector("#qr-box"),
    credentialsSection: document.querySelector("#credentials-section"),
    credentialSubscription: document.querySelector("#credential-subscription"),
    credentialFields: document.querySelector("#credential-fields"),
    credentialJson: document.querySelector("#credential-json"),
    opsSection: document.querySelector("#ops-section"),
    opsSubscription: document.querySelector("#ops-subscription"),
    refreshOps: document.querySelector("#refresh-ops"),
    opsList: document.querySelector("#ops-list"),
  };

  function start(wasm) {
    state.WebBuyer = wasm.WebBuyer;
    state.bolt11QrSvg = wasm.bolt11_qr_svg;
    detectCapabilities();
    renderCapabilities();
    bindEvents();
    setStatus("Enter a relay, operator, and signer mode.");
  }

  function bindEvents() {
    el.configForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      await connect();
    });
    el.refreshListings.addEventListener("click", () => loadListings());
    el.clearOrder.addEventListener("click", () => {
      state.selectedListing = null;
      el.orderSection.hidden = true;
    });
    el.orderForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      await createOrder();
    });
    el.refreshOps.addEventListener("click", () => loadOps());
  }

  function detectCapabilities() {
    const nostr = globalThis.window && window.nostr;
    const nip44 = nostr && nostr.nip44;
    state.capabilities = {
      nostr: Boolean(nostr),
      nip44: Boolean(
        nip44 &&
          typeof nip44.encrypt === "function" &&
          typeof nip44.decrypt === "function",
      ),
      webln: Boolean(globalThis.window && window.webln),
    };
  }

  function renderCapabilities() {
    setPill(
      el.capNostr,
      state.capabilities.nip44 ? "NIP-07: ready" : state.capabilities.nostr ? "NIP-07: no NIP-44" : "NIP-07: absent",
      state.capabilities.nip44 ? "ok" : "warn",
    );
    setPill(el.capWebln, state.capabilities.webln ? "WebLN: ready" : "WebLN: absent", state.capabilities.webln ? "ok" : "warn");
  }

  async function connect() {
    clearError();
    detectCapabilities();
    renderCapabilities();
    const relayUrl = el.relayUrl.value.trim();
    const operatorNpub = el.operatorNpub.value.trim();
    const signerMode = el.signerMode.value;
    state.requestedSignerMode = signerMode;
    setStatus("Connecting...");

    try {
      const buyer = new state.WebBuyer(relayUrl, operatorNpub, signerMode);
      state.buyer = buyer;
      state.resolvedSignerMode = buyer.resolvedSignerMode();
      await renderSignerPanel();
      setStatus("Connected.");
      await loadListings();
    } catch (error) {
      showError(error);
      setStatus("Connection failed.");
    }
  }

  async function retryNip07() {
    const previousMode = el.signerMode.value;
    el.signerMode.value = "nip07";
    try {
      await connect();
    } finally {
      if (!state.buyer || state.resolvedSignerMode !== "nip07") {
        el.signerMode.value = previousMode;
      }
    }
  }

  async function renderSignerPanel() {
    el.signerPanel.hidden = false;
    el.signerPanel.replaceChildren();

    const modeLine = document.createElement("p");
    modeLine.className = "mode-line";
    modeLine.append("Resolved signer: ");
    const mode = document.createElement("strong");
    mode.textContent = state.resolvedSignerMode || "unknown";
    modeLine.append(mode);
    el.signerPanel.append(modeLine);

    try {
      const npub = await state.buyer.buyerNpub();
      const npubLine = document.createElement("p");
      npubLine.className = "small-text";
      npubLine.textContent = `Buyer npub: ${npub}`;
      el.signerPanel.append(npubLine);
    } catch (error) {
      showError(error);
    }

    if (state.requestedSignerMode === "auto" && state.resolvedSignerMode === "embedded") {
      const notice = document.createElement("div");
      notice.className = "notice";
      notice.append(textBlock("NIP-07 was not ready, so this tab is using an embedded key."));
      const retry = button("Retry NIP-07", retryNip07);
      notice.append(retry);
      el.signerPanel.append(notice);
    }

    if (state.resolvedSignerMode === "embedded") {
      const nsec = state.buyer.embeddedNsec();
      if (nsec) {
        const box = document.createElement("div");
        box.className = "backup-box";
        const prompt = document.createElement("strong");
        prompt.textContent = "BACK THIS UP - it decrypts your credentials";
        const secret = document.createElement("code");
        secret.textContent = nsec;
        box.append(prompt, secret, button("Copy nsec", () => copyText(nsec)));
        el.signerPanel.append(box);
      }
    }
  }

  async function loadListings() {
    if (!state.buyer) {
      return;
    }
    clearError();
    setStatus("Loading listings...");
    el.listingsSection.hidden = false;
    el.listings.replaceChildren();
    el.listingCount.textContent = "";

    try {
      const data = await callBuyer(state.buyer.discover());
      state.listings = Array.isArray(data.listings) ? data.listings : [];
      renderListings();
      setStatus("Listings loaded.");
    } catch (error) {
      showError(error);
      setStatus("Listing discovery failed.");
    }
  }

  function renderListings() {
    el.listingCount.textContent = `${state.listings.length} available`;
    el.listings.replaceChildren();

    if (state.listings.length === 0) {
      el.listings.append(empty("No listings returned by this operator."));
      return;
    }

    for (const listing of state.listings) {
      const item = document.createElement("article");
      item.className = "listing";

      const title = document.createElement("h3");
      title.textContent = listing.title || listing.recipe_id || "Untitled listing";
      const price = document.createElement("p");
      price.className = "price";
      price.textContent = `${formatSat(listing.amount_sat)} sat / ${listing.period || "period"}`;
      const id = document.createElement("code");
      id.textContent = listing.listing_id || listing.d || "";
      const summary = document.createElement("p");
      summary.textContent = listing.summary || "";
      const order = button("Order", () => selectListing(listing));

      item.append(title, price, id);
      if (summary.textContent) {
        item.append(summary);
      }
      item.append(order);
      el.listings.append(item);
    }
  }

  function selectListing(listing) {
    state.selectedListing = listing;
    state.invoice = null;
    state.provision = null;
    state.subscriptionId = null;
    el.invoiceSection.hidden = true;
    el.credentialsSection.hidden = true;
    el.opsSection.hidden = true;
    renderOrderForm();
  }

  function renderOrderForm() {
    const listing = state.selectedListing;
    if (!listing) {
      return;
    }
    el.orderSection.hidden = false;
    el.orderTitle.textContent = `${listing.title || listing.listing_id} - ${formatSat(listing.amount_sat)} sat`;
    el.paramFields.replaceChildren();
    el.refundDest.value = "";

    const params = Array.isArray(listing.params) ? listing.params : [];
    if (params.length === 0) {
      el.paramFields.append(empty("This listing has no extra order params."));
    } else {
      for (const param of params) {
        el.paramFields.append(paramInput(param));
      }
    }
    el.orderSection.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  function paramInput(param) {
    const label = document.createElement("label");
    const title = document.createElement("span");
    title.textContent = param.label || param.key;
    label.append(title);
    const key = param.key;
    const type = String(param.type || param.ty || "string").toLowerCase();
    const input = type === "json" ? document.createElement("textarea") : document.createElement("input");
    input.dataset.paramKey = key;
    input.dataset.paramType = type;
    input.required = Boolean(param.required);

    if (type === "bool" || type === "boolean") {
      input.type = "checkbox";
      input.className = "check-input";
      label.className = "check-row";
      label.append(input);
      return label;
    }

    if (type === "int" || type === "integer" || type === "number") {
      input.type = "number";
      input.step = type === "number" ? "any" : "1";
    } else if (type !== "json") {
      input.type = "text";
    }

    label.append(input);
    return label;
  }

  async function createOrder() {
    if (!state.buyer || !state.selectedListing) {
      return;
    }
    clearError();

    const refundDest = el.refundDest.value.trim();
    if (!validRefundDest(refundDest)) {
      showMessageError("bad_request", "Refund destination must be an LN address or HTTPS LNURL, not a raw BOLT11/BOLT12 value.");
      return;
    }

    let params;
    try {
      params = collectParams();
    } catch (error) {
      showMessageError("bad_request", error.message);
      return;
    }

    setStatus("Creating order...");
    try {
      const invoice = await callBuyer(
        state.buyer.create_order(state.selectedListing.listing_id, params, refundDest),
      );
      state.invoice = invoice;
      state.subscriptionId = invoice.order_id || invoice.subscription_id || "";
      renderInvoice();
      // Invoice hand-off is EXPLICIT — the SPA never auto-triggers a payment (a pre-authorized WebLN
      // wallet could otherwise pay without the buyer choosing to). The user clicks "Pay with WebLN"
      // (rendered by renderInvoice) or copies/scans the invoice to pay from their own wallet.
      setStatus(
        state.capabilities.webln
          ? 'Invoice ready — click "Pay with WebLN", or copy/scan it to pay from another wallet.'
          : "Invoice ready — copy or scan the invoice to pay from your wallet.",
      );
    } catch (error) {
      showError(error);
      setStatus("Order failed.");
    }
  }

  function collectParams(root = el.paramFields) {
    const params = {};
    const inputs = root.querySelectorAll("[data-param-key]");
    for (const input of inputs) {
      const key = input.dataset.paramKey;
      const type = input.dataset.paramType;
      if (!key) {
        continue;
      }
      if (input.type === "checkbox") {
        params[key] = input.checked;
        continue;
      }
      const raw = input.value.trim();
      if (!raw) {
        if (input.required) {
          throw new Error(`${key} is required.`);
        }
        continue;
      }
      if (type === "int" || type === "integer") {
        const value = Number.parseInt(raw, 10);
        if (!Number.isFinite(value)) {
          throw new Error(`${key} must be an integer.`);
        }
        params[key] = value;
      } else if (type === "number") {
        const value = Number.parseFloat(raw);
        if (!Number.isFinite(value)) {
          throw new Error(`${key} must be a number.`);
        }
        params[key] = value;
      } else if (type === "json") {
        params[key] = JSON.parse(raw);
      } else {
        params[key] = raw;
      }
    }
    return params;
  }

  function validRefundDest(value) {
    if (!value) {
      return false;
    }
    // Mirror the daemon's intake gate (order_intake.rs detect_form): an '@' means Lightning
    // address and wins over any prefix (a local-part may legitimately start with "lnbc"/"lno"),
    // then HTTPS LNURL or bech32 LNURL. bolt11/bolt12 strings have none of these shapes, so
    // they are rejected by construction — no prefix blacklist.
    if (value.includes("@")) {
      return true;
    }
    const lower = value.toLowerCase();
    return lower.startsWith("https://") || lower.startsWith("lnurl1");
  }

  function renderInvoice() {
    const invoice = state.invoice;
    if (!invoice) {
      return;
    }
    el.invoiceSection.hidden = false;
    el.invoiceSummary.textContent = `${formatSat(invoice.amount_sat)} sat due for ${invoice.period || "subscription"}`;
    el.invoiceMeta.replaceChildren(
      metaItem("Order id", invoice.order_id || ""),
      metaItem("Request id", invoice.request_id || ""),
      metaItem("Expires", formatTime(invoice.expires_at)),
    );
    el.invoiceActions.replaceChildren();
    el.qrBox.replaceChildren();
    el.qrBox.hidden = true;

    if (state.capabilities.webln) {
      setPill(el.invoiceMode, "WebLN", "ok");
      el.invoiceActions.append(button("Pay with WebLN", payWithWebLn));
    } else {
      renderQrFallback();
    }
    el.invoiceSection.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  function renderQrFallback() {
    const bolt11 = state.invoice && state.invoice.bolt11;
    setPill(el.invoiceMode, "Copy / QR", "warn");
    el.invoiceActions.replaceChildren(
      button("Copy BOLT11", () => copyText(bolt11)),
      button("I've paid - wait for credentials", waitForProvision),
    );
    const svg = state.bolt11QrSvg(bolt11 || "");
    if (svg) {
      el.qrBox.hidden = false;
      el.qrBox.innerHTML = svg;
    }
  }

  async function payWithWebLn() {
    if (!state.invoice || !state.invoice.bolt11) {
      return;
    }
    if (state.paying) {
      return; // reentrancy guard: at most one in-flight WebLN sendPayment
    }
    clearError();
    detectCapabilities();
    renderCapabilities();

    if (!state.capabilities.webln) {
      renderQrFallback();
      return;
    }

    state.paying = true;
    setStatus("Opening WebLN wallet...");
    try {
      await window.webln.enable();
      await window.webln.sendPayment(state.invoice.bolt11);
      setStatus("Payment sent. Waiting for credentials...");
      await waitForProvision();
    } catch (error) {
      showMessageError("wallet", error && error.message ? error.message : "WebLN payment did not complete.");
      renderQrFallback();
      setStatus("Use copy or QR to pay with another wallet.");
    } finally {
      state.paying = false;
    }
  }

  async function waitForProvision() {
    if (!state.buyer || !state.subscriptionId || state.waiting) {
      return;
    }
    clearError();
    state.waiting = true;
    renderWaitingActions();
    setStatus("Waiting for credentials...");

    try {
      const provision = await callBuyer(state.buyer.wait_provision(state.subscriptionId));
      state.provision = provision;
      state.subscriptionId = provision.subscription_id || state.subscriptionId;
      renderCredentials();
      await loadOps();
      setStatus("Credentials ready.");
    } catch (error) {
      showError(error);
      renderWaitRetry();
      setStatus("Order id kept. Retry waiting when the payment or delivery should be ready.");
    } finally {
      state.waiting = false;
    }
  }

  function renderWaitingActions() {
    const bolt11 = state.invoice && state.invoice.bolt11;
    el.invoiceActions.replaceChildren(button("Copy BOLT11", () => copyText(bolt11)));
    if (!state.capabilities.webln) {
      el.invoiceActions.append(button("Waiting...", () => {}, true));
    }
  }

  function renderWaitRetry() {
    const bolt11 = state.invoice && state.invoice.bolt11;
    el.invoiceActions.replaceChildren(
      button("Copy BOLT11", () => copyText(bolt11)),
      button("Retry wait", waitForProvision),
    );
    if (!state.capabilities.webln) {
      const svg = state.bolt11QrSvg(bolt11 || "");
      if (svg) {
        el.qrBox.hidden = false;
        el.qrBox.innerHTML = svg;
      }
    }
  }

  function renderCredentials() {
    const ready = state.provision;
    const payload = ready && ready.payload && typeof ready.payload === "object" ? ready.payload : {};
    el.credentialsSection.hidden = false;
    el.credentialSubscription.textContent = `Subscription ${ready.subscription_id || state.subscriptionId}`;
    el.credentialFields.replaceChildren();

    for (const key of ["host", "port", "user", "credential"]) {
      if (payload[key] !== undefined && payload[key] !== null) {
        el.credentialFields.append(metaItem(key, String(payload[key])));
      }
    }

    if (!el.credentialFields.childElementCount) {
      el.credentialFields.append(metaItem("payload", "See JSON payload"));
    }
    el.credentialJson.textContent = JSON.stringify(payload, null, 2);
    el.credentialsSection.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  async function loadOps() {
    if (!state.buyer || !state.subscriptionId || !state.provision) {
      return;
    }
    clearError();
    el.opsSection.hidden = false;
    el.opsSubscription.textContent = `Subscription ${state.subscriptionId}`;
    el.opsList.replaceChildren(empty("Loading ops..."));

    try {
      const data = await callBuyer(state.buyer.list_ops());
      const operations = Array.isArray(data.operations) ? data.operations : [];
      renderOps(operations);
    } catch (error) {
      showError(error);
      el.opsList.replaceChildren(empty("Ops unavailable."));
    }
  }

  function renderOps(operations) {
    el.opsList.replaceChildren();
    if (operations.length === 0) {
      el.opsList.append(empty("No operations declared."));
      return;
    }

    for (const op of operations) {
      const item = document.createElement("article");
      item.className = "op";
      const title = document.createElement("h3");
      title.textContent = op.label || op.name;
      const kind = document.createElement("p");
      kind.className = "small-text";
      kind.textContent = `Kind: ${op.kind}`;
      item.append(title, kind);

      if (op.kind === "request") {
        const fields = document.createElement("div");
        fields.className = "form-stack";
        for (const param of Array.isArray(op.params) ? op.params : []) {
          fields.append(paramInput(param));
        }
        const result = document.createElement("pre");
        result.className = "json-box";
        result.hidden = true;
        item.append(fields, button("Invoke", () => invokeOp(op, fields, result)), result);
      } else {
        const disabled = button("Interactive out of scope", () => {}, true);
        item.classList.add("muted");
        item.append(disabled);
      }
      el.opsList.append(item);
    }
  }

  async function invokeOp(op, fieldRoot, resultEl) {
    clearError();
    let params;
    try {
      params = collectParams(fieldRoot);
    } catch (error) {
      showMessageError("bad_request", error.message);
      return;
    }
    resultEl.hidden = false;
    resultEl.textContent = "Waiting...";
    try {
      const result = await callBuyer(
        state.buyer.invoke_op(state.subscriptionId, op.name, op.kind, params),
      );
      resultEl.textContent = JSON.stringify(result.data, null, 2);
    } catch (error) {
      resultEl.textContent = "";
      resultEl.hidden = true;
      showError(error);
    }
  }

  async function callBuyer(promise) {
    try {
      const envelope = await promise;
      if (envelope && envelope.ok === true) {
        return envelope.data;
      }
      throw envelope;
    } catch (error) {
      throw normalizeError(error);
    }
  }

  function normalizeError(error) {
    if (error && error.ok === false && error.error) {
      return error.error;
    }
    if (error && error.error && typeof error.error === "object") {
      return error.error;
    }
    if (error && typeof error === "object" && "code" in error && "message" in error) {
      return error;
    }
    return {
      code: "error",
      message: error && error.message ? error.message : String(error || "unknown error"),
    };
  }

  function showError(error) {
    const normalized = normalizeError(error);
    showMessageError(normalized.code || "error", normalized.message || "Unknown error");
  }

  function showMessageError(code, message) {
    el.error.hidden = false;
    el.error.replaceChildren();
    const strong = document.createElement("strong");
    strong.textContent = code;
    const text = document.createElement("span");
    text.textContent = message;
    el.error.append(strong, text);
  }

  function clearError() {
    el.error.hidden = true;
    el.error.replaceChildren();
  }

  function setStatus(message) {
    el.status.textContent = message;
  }

  function setPill(node, text, tone) {
    node.textContent = text;
    node.dataset.tone = tone;
  }

  function metaItem(label, value) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = value || "-";
    const fragment = document.createDocumentFragment();
    fragment.append(dt, dd);
    return fragment;
  }

  function button(label, onClick, disabled) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = label;
    btn.disabled = Boolean(disabled);
    btn.addEventListener("click", onClick);
    return btn;
  }

  function textBlock(text) {
    const p = document.createElement("p");
    p.textContent = text;
    return p;
  }

  function empty(text) {
    const p = document.createElement("p");
    p.className = "empty";
    p.textContent = text;
    return p;
  }

  async function copyText(text) {
    if (!text) {
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
      setStatus("Copied.");
    } catch (_error) {
      setStatus("Copy failed.");
    }
  }

  function formatSat(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toLocaleString() : "-";
  }

  function formatTime(unixSeconds) {
    const value = Number(unixSeconds);
    if (!Number.isFinite(value) || value <= 0) {
      return "-";
    }
    return new Date(value * 1000).toLocaleString();
  }

  globalThis.LnRentBuyerWeb = { start };
})();
