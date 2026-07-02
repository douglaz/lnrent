use lnrent_buyer_core::lnrent_wire::{Event, Keys, NostrSigner, PublicKey};
use nostr::{signer::SignerBackend, SignerError, ToBech32, UnsignedEvent};

#[cfg(target_arch = "wasm32")]
use {
    js_sys::{Function, Promise, Reflect},
    wasm_bindgen::{JsCast, JsValue},
    wasm_bindgen_futures::JsFuture,
};

#[cfg(target_arch = "wasm32")]
const EMBEDDED_NSEC_STORAGE_KEY: &str = "lnrent.buyer.embedded_nsec";

#[derive(Debug, Clone)]
pub enum WebSigner {
    Embedded(Keys),
    #[cfg(target_arch = "wasm32")]
    Nip07(Nip07Signer),
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Copy)]
pub struct Nip07Signer;

impl WebSigner {
    pub fn embedded(keys: Keys) -> Self {
        Self::Embedded(keys)
    }

    pub fn embedded_random() -> Self {
        Self::Embedded(Keys::generate())
    }

    pub fn embedded_from_secret(secret: &str) -> Result<Self, String> {
        Keys::parse(secret)
            .map(Self::Embedded)
            .map_err(|e| format!("parsing embedded buyer key: {e}"))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn embedded_from_session_or_generate() -> Result<Self, String> {
        if let Some(nsec) = session_storage_get(EMBEDDED_NSEC_STORAGE_KEY)
            .ok()
            .flatten()
        {
            if let Ok(signer) = Self::embedded_from_secret(&nsec) {
                return Ok(signer);
            }
        }

        let signer = Self::embedded_random();
        if let Some(nsec) = signer
            .embedded_nsec()
            .map_err(|e| format!("encoding embedded buyer nsec: {e}"))?
        {
            let _ = session_storage_set(EMBEDDED_NSEC_STORAGE_KEY, &nsec);
        }
        Ok(signer)
    }

    pub fn embedded_nsec(&self) -> Result<Option<String>, SignerError> {
        match self {
            Self::Embedded(keys) => keys
                .secret_key()
                .to_bech32()
                .map(Some)
                .map_err(|e| SignerError::from(format!("encoding embedded buyer nsec: {e}"))),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(_) => Ok(None),
        }
    }

    pub async fn buyer_npub(&self) -> Result<String, SignerError> {
        self.get_public_key()
            .await?
            .to_bech32()
            .map_err(|e| SignerError::from(format!("encoding buyer npub: {e}")))
    }
}

#[cfg(target_arch = "wasm32")]
impl Nip07Signer {
    pub fn new() -> Result<Self, String> {
        let nostr = nostr_object()?;
        require_function(&nostr, "getPublicKey")?;
        require_function(&nostr, "signEvent")?;
        let nip44 = nip44_object(&nostr)?;
        require_function(&nip44, "encrypt")?;
        require_function(&nip44, "decrypt")?;
        Ok(Self)
    }
}

impl NostrSigner for WebSigner {
    fn backend(&self) -> SignerBackend<'_> {
        match self {
            Self::Embedded(keys) => keys.backend(),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.backend(),
        }
    }

    fn get_public_key(&self) -> nostr::util::BoxedFuture<'_, Result<PublicKey, SignerError>> {
        match self {
            Self::Embedded(keys) => keys.get_public_key(),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.get_public_key(),
        }
    }

    fn sign_event(
        &self,
        unsigned: UnsignedEvent,
    ) -> nostr::util::BoxedFuture<'_, Result<Event, SignerError>> {
        match self {
            Self::Embedded(keys) => keys.sign_event(unsigned),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.sign_event(unsigned),
        }
    }

    fn nip04_encrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        content: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        match self {
            Self::Embedded(keys) => keys.nip04_encrypt(public_key, content),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.nip04_encrypt(public_key, content),
        }
    }

    fn nip04_decrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        encrypted_content: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        match self {
            Self::Embedded(keys) => keys.nip04_decrypt(public_key, encrypted_content),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.nip04_decrypt(public_key, encrypted_content),
        }
    }

    fn nip44_encrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        content: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        match self {
            Self::Embedded(keys) => keys.nip44_encrypt(public_key, content),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.nip44_encrypt(public_key, content),
        }
    }

    fn nip44_decrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        payload: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        match self {
            Self::Embedded(keys) => keys.nip44_decrypt(public_key, payload),
            #[cfg(target_arch = "wasm32")]
            Self::Nip07(signer) => signer.nip44_decrypt(public_key, payload),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl NostrSigner for Nip07Signer {
    fn backend(&self) -> SignerBackend<'_> {
        SignerBackend::BrowserExtension
    }

    fn get_public_key(&self) -> nostr::util::BoxedFuture<'_, Result<PublicKey, SignerError>> {
        Box::pin(async move {
            let nostr = nostr_object_signer()?;
            let get_public_key = function(&nostr, "getPublicKey")?;
            let value = call0(&nostr, &get_public_key).await?;
            let raw = value.as_string().ok_or_else(|| {
                SignerError::from("window.nostr.getPublicKey returned non-string")
            })?;
            PublicKey::parse(&raw)
                .map_err(|e| SignerError::from(format!("NIP-07 public key is invalid: {e}")))
        })
    }

    fn sign_event(
        &self,
        unsigned: UnsignedEvent,
    ) -> nostr::util::BoxedFuture<'_, Result<Event, SignerError>> {
        Box::pin(async move {
            let unsigned_pubkey = unsigned.pubkey;
            let event_value = serde_wasm_bindgen::to_value(&unsigned)
                .map_err(|e| SignerError::from(format!("marshal unsigned event: {e}")))?;
            let nostr = nostr_object_signer()?;
            let sign_event = function(&nostr, "signEvent")?;
            let signed_value = call1(&nostr, &sign_event, &event_value).await?;
            let signed: Event = serde_wasm_bindgen::from_value(signed_value)
                .map_err(|e| SignerError::from(format!("marshal signed event: {e}")))?;

            if signed.pubkey != unsigned_pubkey {
                return Err(SignerError::from(format!(
                    "NIP-07 signed event with pubkey {} but expected {}",
                    signed.pubkey.to_hex(),
                    unsigned_pubkey.to_hex()
                )));
            }
            signed
                .verify()
                .map_err(|e| SignerError::from(format!("NIP-07 signed invalid event: {e}")))?;
            Ok(signed)
        })
    }

    fn nip04_encrypt<'a>(
        &'a self,
        _public_key: &'a PublicKey,
        _content: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        Box::pin(async { Err(SignerError::from("NIP-04 is unsupported by the web buyer")) })
    }

    fn nip04_decrypt<'a>(
        &'a self,
        _public_key: &'a PublicKey,
        _encrypted_content: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        Box::pin(async { Err(SignerError::from("NIP-04 is unsupported by the web buyer")) })
    }

    fn nip44_encrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        content: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        Box::pin(async move {
            let nip44 = nip44_object_signer()?;
            let encrypt = function(&nip44, "encrypt")?;
            let value = call2(
                &nip44,
                &encrypt,
                &JsValue::from_str(&public_key.to_hex()),
                &JsValue::from_str(content),
            )
            .await?;
            value
                .as_string()
                .ok_or_else(|| SignerError::from("window.nostr.nip44.encrypt returned non-string"))
        })
    }

    fn nip44_decrypt<'a>(
        &'a self,
        public_key: &'a PublicKey,
        payload: &'a str,
    ) -> nostr::util::BoxedFuture<'a, Result<String, SignerError>> {
        Box::pin(async move {
            let nip44 = nip44_object_signer()?;
            let decrypt = function(&nip44, "decrypt")?;
            let value = call2(
                &nip44,
                &decrypt,
                &JsValue::from_str(&public_key.to_hex()),
                &JsValue::from_str(payload),
            )
            .await?;
            value
                .as_string()
                .ok_or_else(|| SignerError::from("window.nostr.nip44.decrypt returned non-string"))
        })
    }
}

#[cfg(target_arch = "wasm32")]
fn session_storage_get(key: &str) -> Result<Option<String>, String> {
    let Some(storage) = session_storage()? else {
        return Ok(None);
    };
    let get_item = function_string(&storage, "getItem")?;
    let value = get_item
        .call1(&storage, &JsValue::from_str(key))
        .map_err(|e| format!("reading sessionStorage embedded key: {}", js_value_text(e)))?;
    if value.is_null() || value.is_undefined() {
        Ok(None)
    } else {
        value
            .as_string()
            .map(Some)
            .ok_or_else(|| "sessionStorage embedded key is not a string".to_string())
    }
}

#[cfg(target_arch = "wasm32")]
fn session_storage_set(key: &str, value: &str) -> Result<(), String> {
    let Some(storage) = session_storage()? else {
        return Ok(());
    };
    let set_item = function_string(&storage, "setItem")?;
    set_item
        .call2(&storage, &JsValue::from_str(key), &JsValue::from_str(value))
        .map(|_| ())
        .map_err(|e| format!("writing sessionStorage embedded key: {}", js_value_text(e)))
}

#[cfg(target_arch = "wasm32")]
fn session_storage() -> Result<Option<JsValue>, String> {
    let storage = Reflect::get(&js_sys::global(), &JsValue::from_str("sessionStorage"))
        .map_err(|e| format!("opening sessionStorage: {}", js_value_text(e)))?;
    if storage.is_null() || storage.is_undefined() {
        Ok(None)
    } else {
        Ok(Some(storage))
    }
}

#[cfg(target_arch = "wasm32")]
fn nostr_object_signer() -> Result<JsValue, SignerError> {
    nostr_object().map_err(SignerError::from)
}

#[cfg(target_arch = "wasm32")]
fn nip44_object_signer() -> Result<JsValue, SignerError> {
    nostr_object()
        .and_then(|nostr| nip44_object(&nostr))
        .map_err(SignerError::from)
}

#[cfg(target_arch = "wasm32")]
fn nostr_object() -> Result<JsValue, String> {
    let nostr = Reflect::get(&js_sys::global(), &JsValue::from_str("nostr"))
        .map_err(|e| format!("reading window.nostr: {}", js_value_text(e)))?;
    if nostr.is_null() || nostr.is_undefined() {
        return Err("window.nostr is unavailable".into());
    }
    Ok(nostr)
}

#[cfg(target_arch = "wasm32")]
fn nip44_object(nostr: &JsValue) -> Result<JsValue, String> {
    let nip44 = Reflect::get(nostr, &JsValue::from_str("nip44"))
        .map_err(|e| format!("reading window.nostr.nip44: {}", js_value_text(e)))?;
    if nip44.is_null() || nip44.is_undefined() {
        return Err("window.nostr.nip44 is unavailable".into());
    }
    Ok(nip44)
}

#[cfg(target_arch = "wasm32")]
fn require_function(obj: &JsValue, name: &str) -> Result<(), String> {
    function_string(obj, name).map(|_| ())
}

#[cfg(target_arch = "wasm32")]
fn function_string(obj: &JsValue, name: &str) -> Result<Function, String> {
    let value = Reflect::get(obj, &JsValue::from_str(name))
        .map_err(|e| format!("reading JS method {name}: {}", js_value_text(e)))?;
    value
        .dyn_into::<Function>()
        .map_err(|_| format!("JS method {name} is unavailable"))
}

#[cfg(target_arch = "wasm32")]
fn function(obj: &JsValue, name: &str) -> Result<Function, SignerError> {
    let value = Reflect::get(obj, &JsValue::from_str(name)).map_err(|e| {
        SignerError::from(format!(
            "reading NIP-07 method {name}: {}",
            js_value_text(e)
        ))
    })?;
    value
        .dyn_into::<Function>()
        .map_err(|_| SignerError::from(format!("NIP-07 method {name} is unavailable")))
}

#[cfg(target_arch = "wasm32")]
async fn call0(this: &JsValue, function: &Function) -> Result<JsValue, SignerError> {
    let value = function
        .call0(this)
        .map_err(|e| SignerError::from(format!("calling NIP-07 method: {}", js_value_text(e))))?;
    await_promise(value).await
}

#[cfg(target_arch = "wasm32")]
async fn call1(this: &JsValue, function: &Function, arg: &JsValue) -> Result<JsValue, SignerError> {
    let value = function
        .call1(this, arg)
        .map_err(|e| SignerError::from(format!("calling NIP-07 method: {}", js_value_text(e))))?;
    await_promise(value).await
}

#[cfg(target_arch = "wasm32")]
async fn call2(
    this: &JsValue,
    function: &Function,
    a: &JsValue,
    b: &JsValue,
) -> Result<JsValue, SignerError> {
    let value = function
        .call2(this, a, b)
        .map_err(|e| SignerError::from(format!("calling NIP-07 method: {}", js_value_text(e))))?;
    await_promise(value).await
}

#[cfg(target_arch = "wasm32")]
async fn await_promise(value: JsValue) -> Result<JsValue, SignerError> {
    JsFuture::from(Promise::resolve(&value))
        .await
        .map_err(|e| SignerError::from(format!("awaiting NIP-07 promise: {}", js_value_text(e))))
}

#[cfg(target_arch = "wasm32")]
fn js_value_text(value: JsValue) -> String {
    if let Some(text) = value.as_string() {
        return text;
    }
    js_sys::JSON::stringify(&value)
        .ok()
        .and_then(|s| s.as_string())
        .unwrap_or_else(|| format!("{value:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Kind};
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    const BUYER_SECRET: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const OTHER_SECRET: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    #[test]
    fn embedded_signer_signs_event_for_buyer_key() {
        let signer = WebSigner::embedded_from_secret(BUYER_SECRET).expect("fixed buyer key");
        let buyer = ready(signer.get_public_key()).expect("buyer pubkey");
        let event = ready(
            signer
                .sign_event(EventBuilder::new(Kind::Custom(27272), "web signer test").build(buyer)),
        )
        .expect("signed event");

        assert_eq!(event.pubkey, buyer);
        event.verify().expect("signature verifies");
        assert!(ready(signer.buyer_npub()).unwrap().starts_with("npub1"));
    }

    #[test]
    fn embedded_signer_nip44_round_trips_with_another_key() {
        let buyer = WebSigner::embedded_from_secret(BUYER_SECRET).expect("fixed buyer key");
        let other = WebSigner::embedded_from_secret(OTHER_SECRET).expect("fixed other key");
        let buyer_pubkey = ready(buyer.get_public_key()).unwrap();
        let other_pubkey = ready(other.get_public_key()).unwrap();

        let encrypted = ready(buyer.nip44_encrypt(&other_pubkey, "delivered credentials"))
            .expect("encrypt to other key");
        let decrypted =
            ready(other.nip44_decrypt(&buyer_pubkey, &encrypted)).expect("decrypt from buyer key");

        assert_eq!(decrypted, "delivered credentials");
    }

    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("embedded signer futures should complete synchronously"),
        }
    }
}
