use serde::Serialize;
use sha2::{Digest, Sha256};
use solver_core::{SolveBatchResult, SolveResult};
use uuid::Uuid;

/// Compressed result artifact format persisted in object storage.
pub const ARTIFACT_FORMAT: &str = "msgpack+zstd:v1";
/// File extension for compressed artifacts.
pub const ARTIFACT_EXTENSION: &str = "msgpack.zst";
/// MIME type used for uploads.
pub const ARTIFACT_CONTENT_TYPE: &str = "application/x-msgpack";

#[derive(Debug, Clone, Serialize)]
struct ArtifactEnvelope<T>
where
    T: Serialize,
{
    version: u8,
    format: &'static str,
    snapshot_id: Uuid,
    job_id: Uuid,
    payload: T,
}

/// Encoded artifact bytes and checksums.
#[derive(Debug, Clone)]
pub struct EncodedArtifact {
    /// Compressed bytes.
    pub bytes: Vec<u8>,
    /// SHA-256 checksum in hex.
    pub sha256: String,
    /// Format identifier.
    pub format: &'static str,
    /// Content type for upload.
    pub content_type: &'static str,
    /// Recommended file extension.
    pub extension: &'static str,
}

/// Encodes one solve result as `MessagePack + zstd`.
pub fn encode_solve_one_artifact(
    snapshot_id: Uuid,
    job_id: Uuid,
    result: &SolveResult,
) -> anyhow::Result<EncodedArtifact> {
    let envelope = ArtifactEnvelope {
        version: 1,
        format: ARTIFACT_FORMAT,
        snapshot_id,
        job_id,
        payload: result,
    };
    encode(&envelope)
}

/// Encodes batch solve result as `MessagePack + zstd`.
pub fn encode_solve_batch_artifact(
    snapshot_id: Uuid,
    job_id: Uuid,
    result: &SolveBatchResult,
) -> anyhow::Result<EncodedArtifact> {
    let envelope = ArtifactEnvelope {
        version: 1,
        format: ARTIFACT_FORMAT,
        snapshot_id,
        job_id,
        payload: result,
    };
    encode(&envelope)
}

fn encode<T>(value: &T) -> anyhow::Result<EncodedArtifact>
where
    T: Serialize,
{
    let msgpack = rmp_serde::to_vec_named(value)?;
    let compressed = zstd::encode_all(msgpack.as_slice(), 3)?;
    let mut hasher = Sha256::new();
    hasher.update(&compressed);
    let digest = format!("{:x}", hasher.finalize());

    Ok(EncodedArtifact {
        bytes: compressed,
        sha256: digest,
        format: ARTIFACT_FORMAT,
        content_type: ARTIFACT_CONTENT_TYPE,
        extension: ARTIFACT_EXTENSION,
    })
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use solver_core::{FactorizationState, SolveBatchResult, SolveResult};
    use uuid::Uuid;

    use super::{
        ARTIFACT_CONTENT_TYPE, ARTIFACT_EXTENSION, ARTIFACT_FORMAT, encode_solve_batch_artifact,
        encode_solve_one_artifact,
    };

    #[derive(Debug, Deserialize)]
    struct EnvelopeOne {
        version: u8,
        format: String,
        snapshot_id: Uuid,
        job_id: Uuid,
        payload: SolveResult,
    }

    #[derive(Debug, Deserialize)]
    struct EnvelopeBatch {
        version: u8,
        format: String,
        snapshot_id: Uuid,
        job_id: Uuid,
        payload: SolveBatchResult,
    }

    #[test]
    fn encode_solve_one_artifact_metadata_and_payload_roundtrip() {
        let solved = SolveResult {
            x: Some(vec![1.0, 2.0]),
            g: Some(vec![3.0]),
            h: None,
            factorization_state: FactorizationState::Ready,
        };

        let encoded =
            encode_solve_one_artifact(Uuid::nil(), Uuid::nil(), &solved).expect("encode artifact");

        assert_eq!(encoded.format, ARTIFACT_FORMAT);
        assert_eq!(encoded.content_type, ARTIFACT_CONTENT_TYPE);
        assert_eq!(encoded.extension, ARTIFACT_EXTENSION);
        assert_eq!(encoded.sha256.len(), 64);
        assert!(encoded.sha256.chars().all(|ch| ch.is_ascii_hexdigit()));

        let decompressed = zstd::decode_all(encoded.bytes.as_slice()).expect("decompress");
        let envelope: EnvelopeOne = rmp_serde::from_slice(&decompressed).expect("decode msgpack");

        assert_eq!(envelope.version, 1);
        assert_eq!(envelope.format, ARTIFACT_FORMAT);
        assert_eq!(envelope.snapshot_id, Uuid::nil());
        assert_eq!(envelope.job_id, Uuid::nil());
        assert!((envelope.payload.x.as_ref().expect("x exists")[0] - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn encode_solve_batch_artifact_metadata_and_payload_roundtrip() {
        let solved = SolveBatchResult {
            items: vec![SolveResult {
                x: Some(vec![1.0]),
                g: None,
                h: Some(vec![2.0]),
                factorization_state: FactorizationState::Ready,
            }],
        };

        let encoded =
            encode_solve_batch_artifact(Uuid::nil(), Uuid::nil(), &solved).expect("encode batch");

        assert_eq!(encoded.format, ARTIFACT_FORMAT);
        assert_eq!(encoded.content_type, ARTIFACT_CONTENT_TYPE);
        assert_eq!(encoded.extension, ARTIFACT_EXTENSION);
        assert_eq!(encoded.sha256.len(), 64);

        let decompressed = zstd::decode_all(encoded.bytes.as_slice()).expect("decompress");
        let envelope: EnvelopeBatch = rmp_serde::from_slice(&decompressed).expect("decode msgpack");

        assert_eq!(envelope.version, 1);
        assert_eq!(envelope.format, ARTIFACT_FORMAT);
        assert_eq!(envelope.snapshot_id, Uuid::nil());
        assert_eq!(envelope.job_id, Uuid::nil());
        assert!(
            (envelope.payload.items[0].h.as_ref().expect("h exists")[0] - 2.0).abs() < f64::EPSILON
        );
    }
}
