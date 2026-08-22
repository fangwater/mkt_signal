pub const OKX_SERVICE_UPGRADE_NOTICE_CODE: &str = "64008";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OkexNotice {
    pub code: String,
    pub msg: String,
    pub conn_id: Option<String>,
}

impl OkexNotice {
    pub fn is_service_upgrade(&self) -> bool {
        self.code == OKX_SERVICE_UPGRADE_NOTICE_CODE
    }
}

pub fn parse_okex_notice(text: &str) -> Option<OkexNotice> {
    let json = serde_json::from_str::<serde_json::Value>(text).ok()?;
    let event = json.get("event").and_then(|v| v.as_str())?;
    if !event.eq_ignore_ascii_case("notice") {
        return None;
    }

    Some(OkexNotice {
        code: json_value_to_string(json.get("code")).unwrap_or_default(),
        msg: json_value_to_string(json.get("msg")).unwrap_or_default(),
        conn_id: json_value_to_string(json.get("connId")),
    })
}

fn json_value_to_string(value: Option<&serde_json::Value>) -> Option<String> {
    match value? {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_okex_notice, OKX_SERVICE_UPGRADE_NOTICE_CODE};

    #[test]
    fn parses_service_upgrade_notice() {
        let notice = parse_okex_notice(
            r#"{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade. Please reconnect.","connId":"a4d3ae55"}"#,
        )
        .expect("notice");

        assert_eq!(notice.code, OKX_SERVICE_UPGRADE_NOTICE_CODE);
        assert!(notice.is_service_upgrade());
        assert_eq!(notice.conn_id.as_deref(), Some("a4d3ae55"));
    }

    #[test]
    fn ignores_non_notice_events() {
        assert!(parse_okex_notice(r#"{"event":"subscribe","code":"0"}"#).is_none());
    }
}
