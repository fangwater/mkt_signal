//! 辅助工具函数模块
//!
//! 提供通用的辅助函数：
//! - approx_equal: 浮点数近似相等比较
//! - approx_equal_slice: 浮点数数组近似相等比较
//! - parse_numeric_list: 解析数字列表（支持 JSON 数组、逗号分隔、单个数字）

/// 浮点数近似相等比较
pub fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-12
}

/// 浮点数数组近似相等比较
pub fn approx_equal_slice(a: &[f64], b: &[f64]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| approx_equal(*x, *y))
}

/// 解析数字列表（支持 JSON 数组、逗号分隔、单个数字）
///
/// # Examples
/// ```
/// use mkt_signal::funding_rate::utils::parse_numeric_list;
///
/// // JSON 数组
/// assert_eq!(parse_numeric_list("[1.0, 2.0, 3.0]").unwrap(), vec![1.0, 2.0, 3.0]);
///
/// // 逗号分隔
/// assert_eq!(parse_numeric_list("1.0, 2.0, 3.0").unwrap(), vec![1.0, 2.0, 3.0]);
///
/// // 单个数字
/// assert_eq!(parse_numeric_list("1.0").unwrap(), vec![1.0]);
/// ```
pub fn parse_numeric_list(raw: &str) -> Result<Vec<f64>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<f64>>(trimmed)
            .map_err(|err| format!("JSON array parse error: {err}"))
    } else if trimmed.contains(',') {
        let mut out = Vec::new();
        for part in trimmed.split(',') {
            let piece = part.trim();
            if piece.is_empty() {
                continue;
            }
            match piece.parse::<f64>() {
                Ok(v) => out.push(v),
                Err(err) => {
                    return Err(format!("invalid float '{}': {}", piece, err));
                }
            }
        }
        Ok(out)
    } else {
        trimmed
            .parse::<f64>()
            .map(|v| vec![v])
            .map_err(|err| format!("invalid float: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_equal() {
        assert!(approx_equal(1.0, 1.0));
        assert!(approx_equal(1.0, 1.0 + 1e-13));
        assert!(!approx_equal(1.0, 1.0 + 1e-10));
    }

    #[test]
    fn test_approx_equal_slice() {
        assert!(approx_equal_slice(&[1.0, 2.0], &[1.0, 2.0]));
        assert!(approx_equal_slice(&[1.0, 2.0], &[1.0 + 1e-13, 2.0 - 1e-13]));
        assert!(!approx_equal_slice(&[1.0, 2.0], &[1.0, 2.1]));
        assert!(!approx_equal_slice(&[1.0], &[1.0, 2.0]));
    }

    #[test]
    fn test_parse_numeric_list_json() {
        assert_eq!(
            parse_numeric_list("[1.0, 2.0, 3.0]").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn test_parse_numeric_list_comma() {
        assert_eq!(
            parse_numeric_list("1.0, 2.0, 3.0").unwrap(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[test]
    fn test_parse_numeric_list_single() {
        assert_eq!(parse_numeric_list("1.0").unwrap(), vec![1.0]);
    }

    #[test]
    fn test_parse_numeric_list_empty() {
        assert_eq!(parse_numeric_list("").unwrap(), Vec::<f64>::new());
    }

    #[test]
    fn test_parse_numeric_list_invalid() {
        assert!(parse_numeric_list("invalid").is_err());
        assert!(parse_numeric_list("[1.0, invalid]").is_err());
    }
}
