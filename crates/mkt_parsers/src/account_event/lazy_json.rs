use bytes::Bytes;
use sonic_rs::{get_from_slice, JsonValueTrait, LazyValue};

#[inline]
pub(crate) fn root_from_bytes(msg: &Bytes) -> Option<LazyValue<'_>> {
    get_from_slice(msg.as_ref(), std::iter::empty::<&str>()).ok()
}

#[inline]
pub(crate) fn parse_f64(value: &LazyValue<'_>) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .or_else(|| value.as_u64().map(|v| v as f64))
        .or_else(|| value.as_str().and_then(parse_f64_str))
}

#[inline]
pub(crate) fn parse_i64(value: &LazyValue<'_>) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().map(|v| v as i64))
        .or_else(|| value.as_str().and_then(parse_i64_str))
}

#[inline]
pub(crate) fn get_f64(obj: &LazyValue<'_>, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| obj.get(*key).and_then(|value| parse_f64(&value)))
}

#[inline]
pub(crate) fn get_i64(obj: &LazyValue<'_>, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| obj.get(*key).and_then(|value| parse_i64(&value)))
}

#[inline]
pub(crate) fn get_string(obj: &LazyValue<'_>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        obj.get(*key)
            .and_then(|value| value.as_str().map(|s| s.to_string()))
    })
}

#[inline]
pub(crate) fn field_present_non_empty(obj: &LazyValue<'_>, key: &str) -> bool {
    obj.get(key)
        .map(|value| match value.as_str() {
            Some(s) => !s.trim().is_empty(),
            None => !value.is_null(),
        })
        .unwrap_or(false)
}

pub(crate) fn collect_data_values<'a>(root: &'a LazyValue<'_>) -> Vec<LazyValue<'a>> {
    let Some(data) = root.get("data") else {
        return Vec::new();
    };

    if data.is_object() {
        return vec![data];
    }

    let Some(iter) = data.into_array_iter() else {
        return Vec::new();
    };

    iter.filter_map(Result::ok)
        .filter(|value| value.is_object())
        .collect()
}

#[inline]
fn parse_f64_str(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

#[inline]
fn parse_i64_str(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}
