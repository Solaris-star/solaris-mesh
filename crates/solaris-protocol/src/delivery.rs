use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

const DELIVERY_FIELD: &str = "delivery";
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[derive(Debug, Error)]
pub enum DeliveryEnvelopeError {
    #[error("invalid protocol event payload")]
    InvalidPayload(#[source] serde_json::Error),
    #[error("protocol event payload must be an object")]
    PayloadMustBeObject,
    #[error("protocol event payload contains reserved delivery metadata")]
    ReservedDeliveryMetadata,
    #[error("protocol event delivery digest does not match")]
    DigestMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryMetadata {
    pub delivery_id: String,
    pub msg_id: String,
    pub run_epoch: u64,
    pub sequence: u64,
    pub digest: String,
}

/// A delivery-aware event keeps the original event fields at the top level.
/// Older Hosts can ignore the additional `delivery` object for one version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolEnvelope {
    #[serde(flatten)]
    pub event: Map<String, Value>,
    pub delivery: DeliveryMetadata,
}

impl ProtocolEnvelope {
    pub fn from_event_payload(payload: &[u8], delivery: DeliveryMetadata) -> Result<Self, DeliveryEnvelopeError> {
        let event = event_map_from_payload(payload)?;
        if canonical_event_digest(&event) != delivery.digest {
            return Err(DeliveryEnvelopeError::DigestMismatch);
        }
        Ok(Self { event, delivery })
    }
}

/// Encodes an event map as compact canonical JSON.
///
/// Object keys are recursively sorted by their UTF-8 bytes, array order is
/// preserved, and the reserved top-level `delivery` field is omitted. JSON
/// strings stay strings and JSON numbers stay numbers.
pub fn canonical_event_bytes(event: &Map<String, Value>) -> Vec<u8> {
    let mut output = Vec::new();
    write_object(event, true, &mut output);
    output
}

/// Returns the lowercase SHA-256 marker for the canonical event bytes.
pub fn canonical_event_digest(event: &Map<String, Value>) -> String {
    digest_marker(&canonical_event_digest_bytes(event))
}

/// Parses an event payload and returns the SHA-256 of its canonical event bytes.
pub fn canonical_event_payload_digest(payload: &[u8]) -> Result<[u8; 32], DeliveryEnvelopeError> {
    event_map_from_payload(payload).map(|event| canonical_event_digest_bytes(&event))
}

fn event_map_from_payload(payload: &[u8]) -> Result<Map<String, Value>, DeliveryEnvelopeError> {
    let Value::Object(event) = serde_json::from_slice(payload).map_err(DeliveryEnvelopeError::InvalidPayload)? else {
        return Err(DeliveryEnvelopeError::PayloadMustBeObject);
    };
    if event.contains_key(DELIVERY_FIELD) {
        return Err(DeliveryEnvelopeError::ReservedDeliveryMetadata);
    }
    Ok(event)
}

fn canonical_event_digest_bytes(event: &Map<String, Value>) -> [u8; 32] {
    Sha256::digest(canonical_event_bytes(event)).into()
}

fn write_value(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(value) => write_string(value, output),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_value(value, output);
            }
            output.push(b']');
        }
        Value::Object(values) => write_object(values, false, output),
    }
}

fn write_object(values: &Map<String, Value>, omit_delivery: bool, output: &mut Vec<u8>) {
    let mut entries: Vec<_> = values
        .iter()
        .filter(|(key, _)| !omit_delivery || key.as_str() != DELIVERY_FIELD)
        .collect();
    entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    output.push(b'{');
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        write_string(key, output);
        output.push(b':');
        write_value(value, output);
    }
    output.push(b'}');
}

fn write_string(value: &str, output: &mut Vec<u8>) {
    output.push(b'"');
    for character in value.chars() {
        match character {
            '"' => output.extend_from_slice(br#"\""#),
            '\\' => output.extend_from_slice(br#"\\"#),
            '\u{08}' => output.extend_from_slice(br"\b"),
            '\u{0c}' => output.extend_from_slice(br"\f"),
            '\n' => output.extend_from_slice(br"\n"),
            '\r' => output.extend_from_slice(br"\r"),
            '\t' => output.extend_from_slice(br"\t"),
            character if character <= '\u{1f}' => {
                let code = character as usize;
                output.extend_from_slice(br"\u00");
                output.push(HEX_DIGITS[code >> 4]);
                output.push(HEX_DIGITS[code & 0x0f]);
            }
            character => {
                let mut encoded = [0; 4];
                output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            }
        }
    }
    output.push(b'"');
}

fn digest_marker(digest: &[u8]) -> String {
    let mut marker = String::with_capacity(7 + digest.len() * 2);
    marker.push_str("sha256:");
    for byte in digest {
        marker.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        marker.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    marker
}
