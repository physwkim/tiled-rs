//! RFC 7464 application/json-seq serializer.
//!
//! The Bluesky `/documents/...` endpoint emits ND-JSON today; this
//! serializer produces the standard json-seq framing (RS = 0x1E) so
//! clients that strictly expect application/json-seq parse correctly.
//! Input format: a `serde_json::Value` array; each element becomes one
//! framed record.

use bytes::Bytes;

use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializerFn};

const RS: u8 = 0x1E;
const LF: u8 = 0x0A;

pub const APPLICATION_JSON_SEQ: &str = "application/json-seq";

pub fn register_json_seq_serializer(reg: &SerializationRegistry) {
    // Json-seq is only meaningful for containers (which is how the
    // Bluesky documents endpoint is exposed).
    reg.register(
        StructureFamily::Container,
        APPLICATION_JSON_SEQ,
        json_seq_serializer(),
    );
    reg.register_alias(".jsonseq", APPLICATION_JSON_SEQ);
}

fn json_seq_serializer() -> SerializerFn {
    Box::new(
        |data, _meta| -> Result<Bytes, crate::serialization::registry::SerializeError> {
            // The container handler hands us a JSON-encoded Vec<Value>. Walk
            // it and re-emit each element with proper RS/LF framing.
            let value: serde_json::Value =
                serde_json::from_slice(data).map_err(|e| format!("decode input: {e}"))?;
            let arr = value
                .as_array()
                .ok_or("json-seq input must be a JSON array")?;
            let mut out = Vec::with_capacity(data.len() + arr.len() * 2);
            for record in arr {
                let serialised =
                    serde_json::to_vec(record).map_err(|e| format!("encode record: {e}"))?;
                out.push(RS);
                out.extend_from_slice(&serialised);
                out.push(LF);
            }
            Ok(Bytes::from(out))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_each_record() {
        let reg = SerializationRegistry::new();
        register_json_seq_serializer(&reg);
        let serializer = reg
            .dispatch(StructureFamily::Container, APPLICATION_JSON_SEQ)
            .unwrap();
        let input = serde_json::to_vec(&serde_json::json!([
            {"name": "start"},
            {"name": "stop"}
        ]))
        .unwrap();
        let out = serializer(&input, &serde_json::Value::Null).unwrap();
        assert_eq!(out[0], RS);
        // First record ends with LF.
        assert_eq!(out.iter().filter(|b| **b == LF).count(), 2);
        // Second RS marker present.
        assert!(out.iter().filter(|b| **b == RS).count() == 2);
    }
}
