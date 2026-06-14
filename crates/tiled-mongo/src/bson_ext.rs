//! BSON numeric-read helpers that accept both Int32 and Int64.
//!
//! `Bson::as_i64()` / `Bson::as_f64()` match only the exact BSON type they
//! are named after. pymongo (and several other drivers) emit Int32 for
//! integers that fit in 32 bits, so relying on the single-type accessors
//! silently drops those values. Use these helpers everywhere a numeric BSON
//! field must be read.

use mongodb::bson::Bson;

/// Read an integer from BSON, accepting Int32 or Int64.
pub fn bson_to_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(n) => Some(i64::from(*n)),
        Bson::Int64(n) => Some(*n),
        _ => None,
    }
}

/// Read a float from BSON, accepting Int32, Int64, or Double.
pub fn bson_to_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(n) => Some(f64::from(*n)),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- bson_to_i64 ----

    #[test]
    fn i64_from_int32() {
        assert_eq!(bson_to_i64(&Bson::Int32(480)), Some(480));
    }

    #[test]
    fn i64_from_int64() {
        assert_eq!(
            bson_to_i64(&Bson::Int64(1_000_000_000)),
            Some(1_000_000_000)
        );
    }

    #[test]
    fn i64_rejects_double() {
        assert_eq!(bson_to_i64(&Bson::Double(1.5)), None);
    }

    #[test]
    fn i64_rejects_string() {
        assert_eq!(bson_to_i64(&Bson::String("1".into())), None);
    }

    /// Int32 shape dims are what pymongo emits for small integers; they must
    /// not be silently dropped when building shape vectors.
    #[test]
    fn shape_from_int32_array() {
        let arr = [Bson::Int32(480), Bson::Int32(640)];
        let shape: Vec<usize> = arr
            .iter()
            .filter_map(|v| bson_to_i64(v).map(|n| n as usize))
            .collect();
        assert_eq!(shape, vec![480, 640]);
    }

    #[test]
    fn shape_mixed_int32_int64() {
        let arr = [Bson::Int64(4), Bson::Int32(1024)];
        let shape: Vec<usize> = arr
            .iter()
            .filter_map(|v| bson_to_i64(v).map(|n| n as usize))
            .collect();
        assert_eq!(shape, vec![4, 1024]);
    }

    // ---- bson_to_f64 ----

    #[test]
    fn f64_from_int32() {
        assert_eq!(bson_to_f64(&Bson::Int32(42)), Some(42.0));
    }

    #[test]
    fn f64_from_int64() {
        assert_eq!(bson_to_f64(&Bson::Int64(1_000_000)), Some(1_000_000.0));
    }

    #[test]
    fn f64_from_double() {
        assert_eq!(bson_to_f64(&Bson::Double(1.5)), Some(1.5));
    }

    #[test]
    fn f64_rejects_string() {
        assert_eq!(bson_to_f64(&Bson::String("42".into())), None);
    }

    #[test]
    fn f64_from_int32_is_finite() {
        let result = bson_to_f64(&Bson::Int32(99)).unwrap();
        assert!(result.is_finite(), "Int32 → f64 must not be NaN");
    }
}
