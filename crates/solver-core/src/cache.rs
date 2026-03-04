use std::{sync::Arc, time::SystemTime};

use blake3::Hasher;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use suitesparse_ffi::{CscMatrix, UmfpackFactorization};
use uuid::Uuid;

use crate::validator::ValidationReport;

/// Solver backend options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SolverBackend {
    /// `SuiteSparse` UMFPACK backend.
    Umfpack,
}

/// Cache state for one factorization key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorizationState {
    /// Job is queued but not started.
    Pending,
    /// Factorization in progress.
    Building,
    /// Factorization is ready for solve.
    Ready,
    /// Factorization failed.
    Failed,
    /// Cache entry is stale and must be rebuilt.
    Stale,
}

/// Cache key driven by model version, backend and options hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactorizationKey {
    /// Model version.
    pub model_version: Uuid,
    /// Backend id.
    pub backend: SolverBackend,
    /// Hash of numeric options.
    pub options_hash: String,
}

impl FactorizationKey {
    /// Constructs cache key.
    #[must_use]
    pub fn new(model_version: Uuid, backend: SolverBackend, options_bytes: &[u8]) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(options_bytes);
        let hash = hasher.finalize();
        Self {
            model_version,
            backend,
            options_hash: hash.to_hex().to_string(),
        }
    }

    /// Stable id string.
    #[must_use]
    pub fn factorization_id(&self) -> String {
        format!(
            "{}:{}:{}",
            self.model_version, self.options_hash, self.backend as u8
        )
    }
}

/// Prepared model bundle stored in cache.
#[derive(Debug)]
pub struct PreparedModel {
    /// UMFPACK factorization handle.
    pub factorization: Arc<UmfpackFactorization>,
    /// B matrix.
    pub b: CscMatrix,
    /// C matrix.
    pub c: CscMatrix,
    /// Validation report.
    pub validation: ValidationReport,
}

#[derive(Debug, Clone)]
struct CacheMeta {
    state: FactorizationState,
    updated_at: SystemTime,
    error_message: Option<String>,
}

#[derive(Debug)]
struct CacheEntry {
    prepared: Option<Arc<PreparedModel>>,
    meta: CacheMeta,
}

/// In-memory factorization cache.
#[derive(Debug, Default)]
pub struct FactorizationCache {
    inner: DashMap<FactorizationKey, CacheEntry>,
}

impl FactorizationCache {
    /// Creates cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Marks key as building.
    pub fn set_building(&self, key: FactorizationKey) {
        self.inner.insert(
            key,
            CacheEntry {
                prepared: None,
                meta: CacheMeta {
                    state: FactorizationState::Building,
                    updated_at: SystemTime::now(),
                    error_message: None,
                },
            },
        );
    }

    /// Inserts ready factorization.
    pub fn set_ready(&self, key: FactorizationKey, prepared: Arc<PreparedModel>) {
        self.inner.insert(
            key,
            CacheEntry {
                prepared: Some(prepared),
                meta: CacheMeta {
                    state: FactorizationState::Ready,
                    updated_at: SystemTime::now(),
                    error_message: None,
                },
            },
        );
    }

    /// Marks failure for key.
    pub fn set_failed(&self, key: FactorizationKey, error_message: String) {
        self.inner.insert(
            key,
            CacheEntry {
                prepared: None,
                meta: CacheMeta {
                    state: FactorizationState::Failed,
                    updated_at: SystemTime::now(),
                    error_message: Some(error_message),
                },
            },
        );
    }

    /// Marks all entries for model as stale.
    #[must_use]
    pub fn invalidate_model(&self, model_version: Uuid) -> usize {
        let keys = self
            .inner
            .iter()
            .filter_map(|entry| {
                (entry.key().model_version == model_version).then_some(entry.key().clone())
            })
            .collect::<Vec<_>>();

        for key in &keys {
            if let Some(mut entry) = self.inner.get_mut(key) {
                entry.meta.state = FactorizationState::Stale;
                entry.meta.updated_at = SystemTime::now();
            }
        }

        keys.len()
    }

    /// Returns prepared model if ready.
    #[must_use]
    pub fn get_ready(&self, key: &FactorizationKey) -> Option<Arc<PreparedModel>> {
        self.inner.get(key).and_then(|entry| {
            if entry.meta.state == FactorizationState::Ready {
                entry.prepared.clone()
            } else {
                None
            }
        })
    }

    /// Returns state for key.
    #[must_use]
    pub fn state(&self, key: &FactorizationKey) -> Option<FactorizationState> {
        self.inner.get(key).map(|entry| entry.meta.state)
    }

    /// Returns optional error string for key.
    #[must_use]
    pub fn error(&self, key: &FactorizationKey) -> Option<String> {
        self.inner
            .get(key)
            .and_then(|entry| entry.meta.error_message.clone())
    }
}
