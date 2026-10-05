//! Domain model: samples, duplicate policy and the exact IEEE-754 rules.
//!
//! # Float rules (do not silently quantize)
//!
//! Every sample value is stored and returned **by raw bit pattern**
//! (`f64::to_bits`). The compressor never performs floating-point arithmetic
//! on values, so:
//!
//! * `NaN` payloads (signaling/quiet, sign bit) are preserved bit-for-bit.
//! * negative zero (`-0.0`, bits `0x8000_0000_0000_0000`) is preserved and is
//!   *distinct* from `+0.0` — equality in the dedup/compressor sense is bit
//!   equality, never IEEE `==`.
//! * `+Inf` / `-Inf` are preserved.
//! * No rounding, normalization or quantization happens anywhere.
//!
//! JSON has no representation for NaN/Inf, so the API returns the hex bit
//! pattern (`"bits": "7ff8000000000000"`) for **every** value plus a `kind`:
//! `finite` values also get `"v": <number>`, non-finite values get `"v": null`.
//! The hex bits are always authoritative.

use serde::{Deserialize, Serialize};

/// One sensor observation. Timestamps are explicit int64 Unix nanoseconds;
/// there is no implicit/ambiguous time unit anywhere on disk or on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub t: i64,
    /// Raw IEEE-754 binary64 bit pattern.
    pub bits: u64,
}

impl Sample {
    pub fn new(t: i64, v: f64) -> Self {
        Self { t, bits: v.to_bits() }
    }

    pub fn from_bits(t: i64, bits: u64) -> Self {
        Self { t, bits }
    }

    pub fn value(&self) -> f64 {
        f64::from_bits(self.bits)
    }
}

/// What happens when the same timestamp is written twice inside one write
/// stream (or one-shot batch). Configured per series, never implicit.
///
/// Segment time ranges are strictly disjoint, so cross-segment duplicates
/// cannot occur; this policy only applies within a single sealed segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DuplicatePolicy {
    /// Keep every occurrence in arrival order (default, lossless).
    #[default]
    KeepAll,
    /// Drop later duplicates.
    KeepFirst,
    /// Drop earlier duplicates, keep the last value for that timestamp.
    KeepLast,
    /// Refuse the write with HTTP 409; nothing is appended.
    Reject,
}

impl DuplicatePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            DuplicatePolicy::KeepAll => "keep_all",
            DuplicatePolicy::KeepFirst => "keep_first",
            DuplicatePolicy::KeepLast => "keep_last",
            DuplicatePolicy::Reject => "reject",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "keep_all" => DuplicatePolicy::KeepAll,
            "keep_first" => DuplicatePolicy::KeepFirst,
            "keep_last" => DuplicatePolicy::KeepLast,
            "reject" => DuplicatePolicy::Reject,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FloatKind {
    Finite,
    PosInf,
    NegInf,
    Nan,
}

pub fn classify(bits: u64) -> FloatKind {
    // IEEE-754 binary64: exponent is bits 52..=62.
    let exponent = (bits >> 52) & 0x7ff;
    let mantissa = bits & 0x000f_ffff_ffff_ffff;
    if exponent != 0x7ff {
        FloatKind::Finite
    } else if mantissa != 0 {
        FloatKind::Nan
    } else if bits & (1 << 63) == 0 {
        FloatKind::PosInf
    } else {
        FloatKind::NegInf
    }
}

/// Wire format for one value. Accepts:
/// * JSON number — must be exactly representable as f64 (an integer mantissa
///   needing >53 bits is rejected rather than rounded);
/// * `"0x..."` / `"..."` — exactly 16 hex digits = raw binary64 bits;
/// * `"nan"`, `"+inf"`, `"-inf"`, `"+infinity"`, `"-infinity"`
///   (case-insensitive). NaN is always the canonical quiet NaN payload; to
///   preserve a specific NaN payload send its bit pattern as hex.
pub fn parse_value_input(v: &serde_json::Value) -> Result<u64, String> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let f = i as f64;
                if f.is_finite() && f as i64 == i {
                    return Ok(f.to_bits());
                }
                return Err(format!(
                    "integer {i} is not exactly representable as f64; send 0x{:016x} (its f64 bits) if rounding is intended, or exact bits otherwise",
                    (i as f64).to_bits()
                ));
            }
            if let Some(u) = n.as_u64() {
                let f = u as f64;
                if f.is_finite() && f as u64 == u {
                    return Ok(f.to_bits());
                }
                return Err(format!("integer {u} is not exactly representable as f64"));
            }
            let f = n
                .as_f64()
                .ok_or_else(|| format!("number {n} is not representable as f64"))?;
            Ok(f.to_bits())
        }
        serde_json::Value::String(s) => parse_value_string(s),
        serde_json::Value::Null => Err(
            "null is not a valid value; send a number, hex bits, or \"nan\"/\"+inf\"/\"-inf\""
                .to_string(),
        ),
        other => Err(format!("unsupported value type: {other}")),
    }
}

fn parse_value_string(s: &str) -> Result<u64, String> {
    let trimmed = s.trim();
    let hex = trimmed.strip_prefix("0x").or_else(|| trimmed.strip_prefix("0X"));
    if let Some(h) = hex {
        let bits = u64::from_str_radix(h, 16)
            .map_err(|_| format!("invalid hex bit pattern: {s}"))?;
        if h.len() != 16 {
            return Err(format!(
                "bit pattern must be exactly 16 hex digits ({} given): {s}",
                h.len()
            ));
        }
        return Ok(bits);
    }
    // Bare 16-hex-digit strings are accepted too, but require the full width
    // to avoid confusing a short number for a bit pattern.
    if trimmed.len() == 16
        && trimmed.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return u64::from_str_radix(trimmed, 16).map_err(|e| e.to_string());
    }
    match trimmed.to_ascii_lowercase().as_str() {
        "nan" => Ok(f64::NAN.to_bits()),
        "+inf" | "inf" | "+infinity" | "infinity" => Ok(f64::INFINITY.to_bits()),
        "-inf" | "-infinity" => Ok(f64::NEG_INFINITY.to_bits()),
        _ => Err(format!(
            "invalid value {s:?}: expected number, \"nan\", \"+inf\"/\"-inf\", or 0x + 16 hex digits"
        )),
    }
}

/// JSON object returned for each sample. `bits` is the authoritative value.
pub fn value_json(bits: u64) -> serde_json::Value {
    let kind = classify(bits);
    serde_json::json!({
        "bits": format!("{bits:016x}"),
        "kind": kind,
        "v": if kind == FloatKind::Finite {
            serde_json::Number::from_f64(f64::from_bits(bits))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn specials_classified_and_bit_exact() {
        assert_eq!(classify(f64::NAN.to_bits()), FloatKind::Nan);
        assert_eq!(classify((-0.0f64).to_bits()), FloatKind::Finite);
        assert_eq!(classify(f64::INFINITY.to_bits()), FloatKind::PosInf);
        assert_eq!(classify(f64::NEG_INFINITY.to_bits()), FloatKind::NegInf);
        // A NaN with a custom payload + sign bit survives classification.
        let custom_nan = 0xfff8_0000_0000_0123u64;
        assert_eq!(classify(custom_nan), FloatKind::Nan);
    }

    #[test]
    fn neg_zero_distinct_from_pos_zero() {
        assert_ne!((0.0f64).to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn parse_input_forms() {
        assert_eq!(
            parse_value_input(&json!("0x8000000000000000")).unwrap(),
            (-0.0f64).to_bits()
        );
        assert_eq!(
            parse_value_input(&json!("-inf")).unwrap(),
            f64::NEG_INFINITY.to_bits()
        );
        assert_eq!(
            parse_value_input(&json!("NaN")).unwrap(),
            f64::NAN.to_bits()
        );
        assert_eq!(parse_value_input(&json!(3.5)).unwrap(), 3.5f64.to_bits());
        // >53-bit integer is refused, never rounded.
        assert!(parse_value_input(&json!(9007199254740993i64)).is_err());
        // Exact 2^53 is fine.
        assert!(parse_value_input(&json!(9007199254740992i64)).is_ok());
        assert!(parse_value_input(&json!("0x12")).is_err());
        assert!(parse_value_input(&json!(null)).is_err());
    }
}
