//! Minimal safe Rust wrapper around `SuiteSparse` UMFPACK for CSC matrices.

mod matrix;
mod umfpack;

pub use matrix::{CscMatrix, MatrixError, MatrixTriplet};
pub use umfpack::{FactorizationStats, UmfpackError, UmfpackFactorization, UmfpackNumericOptions};
