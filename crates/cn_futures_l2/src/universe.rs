//! Maintained research universe. Matches `cn_futures.universe`.

pub const EXCLUDED_RESEARCH_PRODUCTS: &[&str] =
    &["BB", "JR", "LR", "PM", "RI", "RS", "WH", "WR", "ZC"];

/// Additive HFQ may produce finite zero or negative research prices. Those are
/// retained, matching the CME CL/WTI continuous-series convention.
pub const EXCLUDED_HFQ_PRODUCTS: &[&str] = &[];

pub fn is_maintained_product(product_id: &str) -> bool {
    !product_id.is_empty() && !EXCLUDED_RESEARCH_PRODUCTS.contains(&product_id)
}

pub fn is_hfq_product(product_id: &str) -> bool {
    is_maintained_product(product_id) && !EXCLUDED_HFQ_PRODUCTS.contains(&product_id)
}

pub fn product_id(instrument: &str) -> Option<String> {
    let text = instrument.trim();
    if text.is_empty()
        || text.to_ascii_lowercase().contains("efp")
        || text.contains(['&', '-', '_'])
    {
        return None;
    }
    let mut letters = String::new();
    let mut digits_started = false;
    for byte in text.bytes() {
        if byte.is_ascii_alphabetic() {
            if digits_started {
                return None;
            }
            letters.push(byte.to_ascii_uppercase() as char);
        } else if byte.is_ascii_digit() {
            digits_started = true;
        } else {
            return None;
        }
    }
    (!letters.is_empty() && digits_started).then_some(letters)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_letters_and_drops_efp() {
        assert_eq!(product_id("rb2605").as_deref(), Some("RB"));
        assert_eq!(product_id("AP601").as_deref(), Some("AP"));
        assert!(product_id("rbefp").is_none());
        assert!(product_id("SP&rb").is_none());
        assert!(!is_maintained_product("ZC"));
        assert!(is_maintained_product("RB"));
        assert!(is_maintained_product("EC"));
        assert!(is_maintained_product("I"));
        assert!(is_maintained_product("LU"));
        assert!(is_maintained_product("P"));
        assert!(is_hfq_product("EC"));
        assert!(is_hfq_product("I"));
        assert!(is_hfq_product("LU"));
        assert!(is_hfq_product("P"));
        assert!(is_hfq_product("RB"));
    }
}
