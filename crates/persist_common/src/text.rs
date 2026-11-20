//! Fixed-capacity, `Copy` UTF-8 text for native identities (contract, trading
//! day, session, order reference, system and trade ids). The trading hot path
//! copies these instead of allocating; serde reads and writes plain strings, so
//! persisted evidence keeps its original JSON shape.
use anyhow::{ensure, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy)]
pub struct Text<const N: usize> {
    bytes: [u8; N],
    len: u16,
}

impl<const N: usize> Default for Text<N> {
    fn default() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
        }
    }
}

impl<const N: usize> Text<N> {
    const FITS: () = assert!(N <= u16::MAX as usize, "Text capacity must fit in u16");

    pub fn new(value: &str) -> Result<Self> {
        let () = Self::FITS;
        ensure!(value.len() <= N, "native text exceeds {N} bytes");
        let mut text = Self::default();
        text.bytes[..value.len()].copy_from_slice(value.as_bytes());
        text.len = value.len() as u16;
        Ok(text)
    }

    /// A NUL-terminated (or full) SDK field; must be valid UTF-8.
    pub fn from_c(field: &[u8]) -> Result<Self> {
        let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
        Self::new(std::str::from_utf8(&field[..end])?)
    }

    /// Writes `value` in decimal without allocating.
    pub fn decimal(value: u64) -> Result<Self> {
        let mut digits = [0u8; 20];
        let mut at = digits.len();
        let mut rest = value;
        loop {
            at -= 1;
            digits[at] = b'0' + (rest % 10) as u8;
            rest /= 10;
            if rest == 0 {
                break;
            }
        }
        let text = std::str::from_utf8(&digits[at..])?;
        Self::new(text)
    }

    /// Formats without allocating, e.g. `Text::format(format_args!("ctp:{f}:{s}"))`.
    pub fn format(args: fmt::Arguments<'_>) -> Result<Self> {
        let mut text = Self::default();
        fmt::Write::write_fmt(&mut text, args)
            .map_err(|_| anyhow::anyhow!("native text exceeds {N} bytes"))?;
        Ok(text)
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: every constructor copies a complete `&str` (or appends whole
        // `&str` pieces through `fmt::Write`), so the prefix is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[..self.len as usize]) }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<const N: usize> fmt::Write for Text<N> {
    fn write_str(&mut self, piece: &str) -> fmt::Result {
        let at = self.len as usize;
        let end = at + piece.len();
        if end > N {
            return Err(fmt::Error);
        }
        self.bytes[at..end].copy_from_slice(piece.as_bytes());
        self.len = end as u16;
        Ok(())
    }
}
impl<const N: usize> std::ops::Deref for Text<N> {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl<const N: usize> AsRef<str> for Text<N> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl<const N: usize> Borrow<str> for Text<N> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl<const N: usize> PartialEq for Text<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl<const N: usize> Eq for Text<N> {}
impl<const N: usize> PartialEq<str> for Text<N> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}
impl<const N: usize> PartialEq<&str> for Text<N> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}
impl<const N: usize> PartialEq<String> for Text<N> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}
impl<const N: usize> PartialEq<Text<N>> for str {
    fn eq(&self, other: &Text<N>) -> bool {
        self == other.as_str()
    }
}
impl<const N: usize> PartialEq<Text<N>> for &str {
    fn eq(&self, other: &Text<N>) -> bool {
        *self == other.as_str()
    }
}
impl<const N: usize> PartialEq<Text<N>> for String {
    fn eq(&self, other: &Text<N>) -> bool {
        self == other.as_str()
    }
}
impl<const N: usize> PartialOrd for Text<N> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<const N: usize> Ord for Text<N> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
// Hashes exactly like `str`, so maps keyed by Text accept `&str` lookups.
impl<const N: usize> Hash for Text<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}
impl<const N: usize> fmt::Debug for Text<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}
impl<const N: usize> fmt::Display for Text<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
/// Literals only: an over-long literal is a programming error. Runtime data
/// uses `Text::new` / `Text::from_c`, which return errors.
impl<const N: usize> From<&'static str> for Text<N> {
    fn from(value: &'static str) -> Self {
        Self::new(value).expect("literal exceeds native text capacity")
    }
}
impl<const N: usize> TryFrom<String> for Text<N> {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(&value)
    }
}
impl<const N: usize> Serialize for Text<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de, const N: usize> Deserialize<'de> for Text<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Visitor<const N: usize>;
        impl<const N: usize> serde::de::Visitor<'_> for Visitor<N> {
            type Value = Text<N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "UTF-8 text up to {N} bytes")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<Text<N>, E> {
                Text::new(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor::<N>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_round_trips_as_a_string_and_hashes_like_str() {
        let text: Text<16> = Text::new("SHFE").unwrap();
        assert_eq!(serde_json::to_string(&text).unwrap(), "\"SHFE\"");
        assert_eq!(serde_json::from_str::<Text<16>>("\"SHFE\"").unwrap(), text);
        assert!(Text::<3>::new("SHFE").is_err());
        assert!(serde_json::from_str::<Text<3>>("\"SHFE\"").is_err());
        let mut map = std::collections::HashMap::new();
        map.insert(text, 1);
        assert_eq!(map.get("SHFE"), Some(&1));
        assert_eq!(Text::<16>::decimal(0).unwrap().as_str(), "0");
        assert_eq!(
            Text::<20>::decimal(u64::MAX).unwrap().as_str(),
            "18446744073709551615"
        );
        assert!(Text::<16>::decimal(u64::MAX).is_err());
        assert_eq!(
            Text::<16>::format(format_args!("ctp:{}:{}", 1, -7)).unwrap(),
            "ctp:1:-7"
        );
        assert!(Text::<4>::format(format_args!("ctp:{}", 1)).is_err());
        assert_eq!(Text::<16>::from_c(b"rb2701\0garbage").unwrap(), "rb2701");
    }
}
