use memchr::{memchr, memchr2, memchr3, memmem};

pub(crate) fn expect_raw_byte(raw: &[u8], pos: &mut usize, expected: u8) -> Option<()> {
    if raw.get(*pos) == Some(&expected) {
        *pos += 1;
        return Some(());
    }
    skip_ws_at(raw, pos);
    if raw.get(*pos) != Some(&expected) {
        return None;
    }
    *pos += 1;
    Some(())
}

pub(crate) fn consume_raw_literal(raw: &[u8], pos: &mut usize, literal: &[u8]) -> Option<()> {
    if raw.get(*pos..*pos + literal.len()) == Some(literal) {
        *pos += literal.len();
        return Some(());
    }
    skip_ws_at(raw, pos);
    if raw.get(*pos..*pos + literal.len()) != Some(literal) {
        return None;
    }
    *pos += literal.len();
    Some(())
}

pub(crate) fn consume_raw_literal_if(raw: &[u8], pos: &mut usize, literal: &[u8]) -> bool {
    if raw.get(*pos..*pos + literal.len()) == Some(literal) {
        *pos += literal.len();
        return true;
    }
    skip_ws_at(raw, pos);
    if raw.get(*pos..*pos + literal.len()) == Some(literal) {
        *pos += literal.len();
        true
    } else {
        false
    }
}

pub(crate) fn consume_raw_field_separator(raw: &[u8], pos: &mut usize) -> Option<bool> {
    match raw.get(*pos).copied()? {
        b',' => {
            *pos += 1;
            return Some(true);
        }
        b'}' => {
            *pos += 1;
            return Some(false);
        }
        _ => {}
    }
    skip_ws_at(raw, pos);
    match raw.get(*pos).copied()? {
        b',' => {
            *pos += 1;
            Some(true)
        }
        b'}' => {
            *pos += 1;
            Some(false)
        }
        _ => None,
    }
}

pub(crate) fn parse_raw_bool_value(raw: &[u8], pos: &mut usize) -> Option<bool> {
    if consume_raw_literal_if(raw, pos, b"true") {
        Some(true)
    } else if consume_raw_literal_if(raw, pos, b"false") {
        Some(false)
    } else {
        None
    }
}

pub(crate) fn bytes_ends_with(raw: &[u8], suffix: &[u8]) -> bool {
    raw.len() >= suffix.len() && raw[raw.len() - suffix.len()..] == *suffix
}

pub(crate) fn stream_channel_starts_with(stream: &[u8], prefix: &[u8]) -> bool {
    let Some(at) = memchr(b'@', stream) else {
        return false;
    };
    stream
        .get(at + 1..at + 1 + prefix.len())
        .is_some_and(|channel| channel == prefix)
}

pub(crate) fn skip_raw_json_value(raw: &[u8], pos: &mut usize) -> Option<()> {
    let mut scanner = JsonObjectScanner { raw, pos: *pos };
    scanner.skip_value()?;
    *pos = scanner.pos;
    Some(())
}

pub(crate) fn take_raw_json_slice<'a>(raw: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    skip_ws_at(raw, pos);
    let start = *pos;
    skip_raw_json_value(raw, pos)?;
    Some(&raw[start..*pos])
}

pub(crate) fn finish_raw_object(raw: &[u8], pos: &mut usize) -> Option<()> {
    skip_ws_at(raw, pos);
    if raw.get(*pos) == Some(&b'}') {
        *pos += 1;
        return Some(());
    }
    let mut scanner = JsonObjectScanner { raw, pos: *pos };
    scanner.skip_rest_of_object()?;
    *pos = scanner.pos;
    Some(())
}

pub(crate) fn finish_raw_message(raw: &[u8], pos: &mut usize) -> bool {
    if *pos == raw.len() {
        return true;
    }
    skip_ws_at(raw, pos);
    *pos == raw.len()
}

pub(crate) fn parse_raw_number(raw: &[u8], pos: &mut usize) -> Option<f64> {
    let bytes = match raw.get(*pos).copied()? {
        b'"' => take_unescaped_quoted_bytes(raw, pos)?,
        b if is_json_ws(b) => {
            skip_ws_at(raw, pos);
            if raw.get(*pos) == Some(&b'"') {
                take_unescaped_quoted_bytes(raw, pos)?
            } else {
                take_unquoted_json_scalar(raw, pos)?
            }
        }
        _ => take_unquoted_json_scalar(raw, pos)?,
    };
    if bytes.is_empty() {
        return None;
    }
    fast_float::parse::<f64, _>(bytes).ok()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RawJsonLevel {
    pub(crate) price: f64,
    pub(crate) amount: f64,
}

pub(crate) fn raw_json_top_level(raw: &[u8]) -> Option<RawJsonLevel> {
    let raw = trim_ascii(raw);
    if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
        return None;
    }

    let mut pos = 1usize;
    loop {
        skip_ws_at(raw, &mut pos);
        match raw.get(pos).copied()? {
            b']' => return None,
            b',' => pos += 1,
            b'[' => return parse_raw_json_level(raw, &mut pos),
            _ => return None,
        }
    }
}

pub(crate) fn raw_json_levels_count(raw: &[u8]) -> Option<usize> {
    let raw = trim_ascii(raw);
    if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
        return None;
    }

    let mut count = 0usize;
    let mut pos = 1usize;
    loop {
        skip_ws_at(raw, &mut pos);
        match raw.get(pos).copied()? {
            b']' => return Some(count),
            b',' => pos += 1,
            b'[' => {
                parse_raw_json_level(raw, &mut pos)?;
                count += 1;
            }
            _ => return None,
        }
    }
}

pub(crate) fn raw_json_levels_iter(raw: &[u8]) -> Option<RawJsonLevelIter<'_>> {
    RawJsonLevelIter::new(raw)
}

pub(crate) struct RawJsonLevelIter<'a> {
    raw: &'a [u8],
    pos: usize,
    done: bool,
}

impl<'a> RawJsonLevelIter<'a> {
    fn new(raw: &'a [u8]) -> Option<Self> {
        let raw = trim_ascii(raw);
        if raw.first() != Some(&b'[') || raw.last() != Some(&b']') {
            return None;
        }
        Some(Self {
            raw,
            pos: 1,
            done: false,
        })
    }
}

impl Iterator for RawJsonLevelIter<'_> {
    type Item = RawJsonLevel;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            skip_ws_at(self.raw, &mut self.pos);
            match self.raw.get(self.pos).copied()? {
                b']' => {
                    self.done = true;
                    return None;
                }
                b',' => self.pos += 1,
                b'[' => return parse_raw_json_level(self.raw, &mut self.pos),
                _ => {
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

pub(crate) fn parse_raw_json_level(raw: &[u8], pos: &mut usize) -> Option<RawJsonLevel> {
    expect_raw_byte(raw, pos, b'[')?;
    let price = parse_raw_number(raw, pos)?;
    skip_ws_at(raw, pos);
    if raw.get(*pos) != Some(&b',') {
        return None;
    }
    *pos += 1;
    let amount = parse_raw_number(raw, pos)?;
    skip_ws_at(raw, pos);
    if raw.get(*pos) != Some(&b']') {
        return None;
    }
    *pos += 1;
    Some(RawJsonLevel { price, amount })
}

pub(crate) fn parse_raw_key_i64(raw: &[u8], key: u8) -> Option<i64> {
    let mut pos = 0usize;
    while let Some(quote_offset) = memchr(b'"', raw.get(pos..)?) {
        pos += quote_offset + 1;
        let start = pos;
        let mut escaped = false;
        loop {
            let rest = raw.get(pos..)?;
            let offset = memchr2(b'"', b'\\', rest)?;
            pos += offset;
            match raw.get(pos).copied()? {
                b'\\' => {
                    escaped = true;
                    pos += 2;
                    if pos > raw.len() {
                        return None;
                    }
                }
                b'"' => {
                    let end = pos;
                    pos += 1;
                    if !escaped && end == start + 1 && raw[start] == key {
                        let mut value_pos = pos;
                        skip_ws_at(raw, &mut value_pos);
                        if raw.get(value_pos) == Some(&b':') {
                            value_pos += 1;
                            return parse_raw_i64_value(raw, &mut value_pos);
                        }
                    }
                    break;
                }
                _ => return None,
            }
        }
    }
    None
}

pub(crate) fn parse_raw_literal_key_i64(raw: &[u8], key: &[u8]) -> Option<i64> {
    let mut pos = 0usize;
    while let Some(offset) = memmem::find(&raw[pos..], key) {
        let key_start = pos + offset;
        let after_key = key_start + key.len();
        let prev = key_start
            .checked_sub(1)
            .and_then(|idx| raw.get(idx).copied());
        if prev.is_some_and(|b| b == b'\\') {
            pos = after_key;
            continue;
        }
        let mut value_pos = after_key;
        skip_ws_at(raw, &mut value_pos);
        if raw.get(value_pos) == Some(&b':') {
            value_pos += 1;
            return parse_raw_i64_value(raw, &mut value_pos);
        }
        pos = after_key;
    }
    None
}

pub(crate) fn parse_raw_i64_value(raw: &[u8], pos: &mut usize) -> Option<i64> {
    match raw.get(*pos).copied()? {
        b'"' => {
            let bytes = take_unescaped_quoted_bytes(raw, pos)?;
            if bytes.is_empty() {
                return None;
            }
            parse_i64_bytes(bytes)
        }
        b if is_json_ws(b) => {
            skip_ws_at(raw, pos);
            if raw.get(*pos) == Some(&b'"') {
                let bytes = take_unescaped_quoted_bytes(raw, pos)?;
                if bytes.is_empty() {
                    return None;
                }
                parse_i64_bytes(bytes)
            } else {
                parse_unquoted_i64_at(raw, pos)
            }
        }
        _ => parse_unquoted_i64_at(raw, pos),
    }
}

fn parse_unquoted_i64_at(raw: &[u8], pos: &mut usize) -> Option<i64> {
    let (value, len) = atoi_simd::parse_until_invalid::<i64>(raw.get(*pos..)?).ok()?;
    if len == 0 {
        return None;
    }
    match raw.get(*pos + len).copied() {
        Some(b',' | b'}' | b']') | None => {
            *pos += len;
            Some(value)
        }
        Some(b) if is_json_ws(b) => {
            *pos += len;
            Some(value)
        }
        _ => None,
    }
}

pub(crate) fn take_unescaped_quoted_bytes<'a>(raw: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    if raw.get(*pos) != Some(&b'"') {
        return None;
    }
    *pos += 1;
    let start = *pos;
    let rest = raw.get(*pos..)?;
    let offset = memchr2(b'"', b'\\', rest)?;
    match rest[offset] {
        b'\\' => None,
        b'"' => {
            let end = *pos + offset;
            *pos = end + 1;
            Some(&raw[start..end])
        }
        _ => None,
    }
}

pub(crate) fn take_unquoted_json_scalar<'a>(raw: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let start = *pos;
    let rest = raw.get(*pos..)?;
    let offset = memchr3(b',', b'}', b']', rest).unwrap_or(rest.len());
    let end = *pos + offset;
    let mut scalar_end = end;
    while scalar_end > start && is_json_ws(raw[scalar_end - 1]) {
        scalar_end -= 1;
    }
    *pos = scalar_end;
    Some(&raw[start..scalar_end])
}

pub(crate) fn parse_i64_bytes(raw: &[u8]) -> Option<i64> {
    match raw.first().copied() {
        Some(b'+') => atoi_simd::parse::<i64>(&raw[1..]).ok(),
        Some(_) => atoi_simd::parse::<i64>(raw).ok(),
        None => None,
    }
}

pub(crate) fn skip_ws_at(raw: &[u8], pos: &mut usize) {
    while raw.get(*pos).is_some_and(|b| is_json_ws(*b)) {
        *pos += 1;
    }
}

#[inline]
pub(crate) fn is_json_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t')
}

pub(crate) struct RawPayloadObject<'a> {
    scanner: JsonObjectScanner<'a>,
    stream: Option<JsonValue<'a>>,
    first_field: Option<(&'a [u8], JsonValue<'a>)>,
    stream_after: Option<JsonObjectScanner<'a>>,
}

impl<'a> RawPayloadObject<'a> {
    pub(crate) fn next_field(&mut self) -> Option<(&'a [u8], JsonValue<'a>)> {
        self.first_field
            .take()
            .or_else(|| self.scanner.next_field())
    }

    pub(crate) fn stream_value(&mut self) -> Option<JsonValue<'a>> {
        if self.stream.is_none() {
            let mut scanner = self.stream_after.take()?;
            scanner.skip_value()?;
            while let Some(key) = scanner.next_key() {
                if key == b"stream" {
                    self.stream = scanner.take_value();
                    break;
                }
                scanner.skip_value()?;
            }
        }
        self.stream
    }
}

pub(crate) fn raw_payload_object(raw: &[u8]) -> RawPayloadObject<'_> {
    let mut scanner = JsonObjectScanner::new(raw);
    let mut stream = None;
    while let Some(key) = scanner.next_key() {
        if key != b"stream" && key != b"data" && stream.is_none() {
            let Some(value) = scanner.take_value() else {
                break;
            };
            return RawPayloadObject {
                scanner,
                stream: None,
                first_field: Some((key, value)),
                stream_after: None,
            };
        }
        if key == b"stream" {
            if let Some(value) = scanner.take_value() {
                stream = Some(value);
                continue;
            }
            break;
        }
        if key == b"data" {
            if scanner.value_starts_object() {
                let data_scanner = scanner.scanner_at_value();
                let stream_after = stream.is_none().then(|| scanner.scanner_at_value());
                return RawPayloadObject {
                    scanner: data_scanner,
                    stream,
                    first_field: None,
                    stream_after,
                };
            }
            if scanner.skip_value().is_none() {
                break;
            }
            continue;
        }
        if scanner.skip_value().is_none() {
            break;
        }
    }
    RawPayloadObject {
        scanner: JsonObjectScanner::new(raw),
        stream,
        first_field: None,
        stream_after: None,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct JsonValue<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) escaped: bool,
}

impl<'a> JsonValue<'a> {
    pub(crate) fn string_bytes(self) -> Option<&'a [u8]> {
        if self.raw.len() < 2 || self.raw.first() != Some(&b'"') || self.raw.last() != Some(&b'"') {
            return None;
        }
        let inner = &self.raw[1..self.raw.len() - 1];
        if self.escaped {
            return None;
        }
        Some(inner)
    }

    pub(crate) fn string_str(self) -> Option<&'a str> {
        std::str::from_utf8(self.string_bytes()?).ok()
    }

    pub(crate) fn object_bytes(self) -> Option<&'a [u8]> {
        if self.raw.first() == Some(&b'{') && self.raw.last() == Some(&b'}') {
            Some(self.raw)
        } else {
            None
        }
    }

    pub(crate) fn array_bytes(self) -> Option<&'a [u8]> {
        if self.raw.first() == Some(&b'[') && self.raw.last() == Some(&b']') {
            Some(self.raw)
        } else {
            None
        }
    }

    pub(crate) fn number_bytes(self) -> Option<&'a [u8]> {
        if self.escaped {
            return None;
        }
        if self.raw.first() == Some(&b'"') && self.raw.last() == Some(&b'"') {
            Some(&self.raw[1..self.raw.len() - 1])
        } else {
            Some(self.raw)
        }
    }

    pub(crate) fn i64(self) -> Option<i64> {
        parse_i64_bytes(self.number_bytes()?)
    }

    pub(crate) fn f64(self) -> Option<f64> {
        fast_float::parse::<f64, _>(self.number_bytes()?).ok()
    }

    pub(crate) fn bool(self) -> Option<bool> {
        match self.raw {
            b"true" => Some(true),
            b"false" => Some(false),
            _ => None,
        }
    }
}

pub(crate) struct JsonObjectScanner<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> JsonObjectScanner<'a> {
    pub(crate) fn new(raw: &'a [u8]) -> Self {
        Self { raw, pos: 0 }
    }

    pub(crate) fn next_field(&mut self) -> Option<(&'a [u8], JsonValue<'a>)> {
        let key = self.next_key()?;
        let value = self.take_value()?;
        Some((key, value))
    }

    pub(crate) fn next_key(&mut self) -> Option<&'a [u8]> {
        self.skip_object_separators();
        if self.raw.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            return None;
        }
        let key = self.take_string_inner()?;
        self.skip_ws();
        if self.raw.get(self.pos) != Some(&b':') {
            return None;
        }
        self.pos += 1;
        self.skip_ws();
        Some(&self.raw[key.0..key.1])
    }

    pub(crate) fn value_starts_object(&self) -> bool {
        self.raw.get(self.pos) == Some(&b'{')
    }

    pub(crate) fn peek_byte(&self) -> Option<u8> {
        self.raw.get(self.pos).copied()
    }

    pub(crate) fn start_array(&mut self) -> Option<()> {
        if self.raw.get(self.pos) != Some(&b'[') {
            return None;
        }
        self.pos += 1;
        Some(())
    }

    pub(crate) fn scanner_at_value(&self) -> JsonObjectScanner<'a> {
        JsonObjectScanner {
            raw: self.raw,
            pos: self.pos,
        }
    }

    pub(crate) fn take_value(&mut self) -> Option<JsonValue<'a>> {
        let start = self.pos;
        let escaped = self.skip_value()?;
        let end = self.pos;
        Some(JsonValue {
            raw: &self.raw[start..end],
            escaped,
        })
    }

    fn skip_object_separators(&mut self) {
        loop {
            self.skip_ws();
            match self.raw.get(self.pos) {
                Some(b'{') | Some(b',') => self.pos += 1,
                _ => break,
            }
        }
    }

    pub(crate) fn skip_rest_of_object(&mut self) -> Option<()> {
        loop {
            self.skip_ws();
            match self.raw.get(self.pos)? {
                b',' => {
                    self.pos += 1;
                    self.skip_ws();
                    self.take_string_inner()?;
                    self.skip_ws();
                    if self.raw.get(self.pos) != Some(&b':') {
                        return None;
                    }
                    self.pos += 1;
                    self.skip_ws();
                    self.skip_value()?;
                }
                b'}' => {
                    self.pos += 1;
                    return Some(());
                }
                _ => return None,
            }
        }
    }

    pub(crate) fn skip_ws(&mut self) {
        while self.raw.get(self.pos).is_some_and(|b| is_json_ws(*b)) {
            self.pos += 1;
        }
    }

    fn take_string_inner(&mut self) -> Option<(usize, usize)> {
        if self.raw.get(self.pos) != Some(&b'"') {
            return None;
        }
        self.pos += 1;
        let start = self.pos;
        let rest = self.raw.get(self.pos..)?;
        let quote_offset = memchr(b'"', rest)?;
        if memchr(b'\\', &rest[..quote_offset]).is_some() {
            return None;
        }
        let end = self.pos + quote_offset;
        self.pos = end + 1;
        Some((start, end))
    }

    pub(crate) fn skip_value(&mut self) -> Option<bool> {
        match *self.raw.get(self.pos)? {
            b'"' => self.skip_string_value(),
            b'{' | b'[' => {
                self.skip_nested_value()?;
                Some(false)
            }
            _ => {
                take_unquoted_json_scalar(self.raw, &mut self.pos)?;
                (self.pos < self.raw.len()).then_some(false)
            }
        }
    }

    fn skip_string_value(&mut self) -> Option<bool> {
        if self.raw.get(self.pos) != Some(&b'"') {
            return None;
        }
        self.pos += 1;
        let mut escaped = false;
        loop {
            let rest = self.raw.get(self.pos..)?;
            let offset = memchr2(b'"', b'\\', rest)?;
            self.pos += offset;
            match self.raw.get(self.pos).copied()? {
                b'\\' => {
                    escaped = true;
                    self.pos += 2;
                    if self.pos > self.raw.len() {
                        return None;
                    }
                }
                b'"' => {
                    self.pos += 1;
                    return Some(escaped);
                }
                _ => return None,
            }
        }
    }

    fn skip_nested_value(&mut self) -> Option<()> {
        let mut depth = 0usize;
        while let Some(&b) = self.raw.get(self.pos) {
            match b {
                b'"' => {
                    self.skip_string_value()?;
                }
                b'{' | b'[' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1)?;
                    self.pos += 1;
                    if depth == 0 {
                        return Some(());
                    }
                }
                _ => self.pos += 1,
            }
        }
        None
    }
}

pub(crate) fn trim_ascii(mut raw: &[u8]) -> &[u8] {
    while raw.first().is_some_and(|b| is_json_ws(*b)) {
        raw = &raw[1..];
    }
    while raw.last().is_some_and(|b| is_json_ws(*b)) {
        raw = &raw[..raw.len() - 1];
    }
    raw
}
