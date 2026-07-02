use lnrent_buyer_core::Clock;

const REQUEST_ID_RANDOM_BYTES: usize = 16;

/// Browser-compatible clock for buyer-core. The struct is stateless so it can satisfy core's
/// current `Send + Sync` clock bound without carrying any browser JS handles.
#[derive(Debug, Default, Clone, Copy)]
pub struct BrowserClock;

impl Clock for BrowserClock {
    fn now_secs(&self) -> i64 {
        now_secs()
    }

    fn new_request_id(&self) -> String {
        let mut bytes = [0u8; REQUEST_ID_RANDOM_BYTES];
        getrandom::getrandom(&mut bytes).expect("secure browser random source is unavailable");
        let id = request_id_from_bytes(&bytes);
        debug_assert!(request_id_has_f4_shape(&id));
        id
    }
}

#[cfg(target_arch = "wasm32")]
fn now_secs() -> i64 {
    (js_sys::Date::now() / 1_000.0) as i64
}

#[cfg(not(target_arch = "wasm32"))]
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn request_id_from_bytes(bytes: &[u8; REQUEST_ID_RANDOM_BYTES]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut id = String::with_capacity("req-".len() + bytes.len() * 2);
    id.push_str("req-");
    for byte in bytes {
        id.push(HEX[(byte >> 4) as usize] as char);
        id.push(HEX[(byte & 0x0f) as usize] as char);
    }
    id
}

pub(crate) fn request_id_has_f4_shape(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generated_request_ids_match_f4_shape_and_are_unique() {
        let clock = BrowserClock;
        let mut seen = HashSet::new();

        for _ in 0..64 {
            let id = clock.new_request_id();
            assert!(
                request_id_has_f4_shape(&id),
                "{id:?} must match ^[A-Za-z0-9_-]{{1,128}}$"
            );
            assert!(seen.insert(id), "fresh request ids must be unique");
        }
    }

    #[test]
    fn request_id_shape_is_pure_and_stable() {
        let id = request_id_from_bytes(&[0xab; REQUEST_ID_RANDOM_BYTES]);

        assert_eq!(id, "req-abababababababababababababababab");
        assert!(request_id_has_f4_shape(&id));
    }
}
