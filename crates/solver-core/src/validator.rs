use serde::{Deserialize, Serialize};
use suitesparse_ffi::CscMatrix;

/// Validator status for matrix pre-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    /// Matrix is structurally valid.
    Ok,
    /// Matrix is valid but may be numerically risky.
    WarningNearSingular,
    /// Matrix has invalid structure and cannot be factorized.
    FailedInvalidStructure,
}

/// Matrix shape and sparsity stats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixStats {
    /// Number of rows.
    pub nrows: i32,
    /// Number of columns.
    pub ncols: i32,
    /// Number of non-zeros.
    pub nnz: usize,
    /// Sparsity ratio in `[0, 1]`.
    pub sparsity: f64,
    /// Empty columns.
    pub empty_columns: usize,
    /// Empty rows.
    pub empty_rows: usize,
    /// Diagonal entries with absolute value below threshold.
    pub near_zero_diagonal: usize,
}

/// Validation report consumed by factorization manager.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Status.
    pub status: ValidationStatus,
    /// Matrix stats.
    pub stats: MatrixStats,
    /// Human-readable warnings and errors.
    pub messages: Vec<String>,
}

/// Validates matrix before UMFPACK factorization.
#[must_use]
pub fn validate_factorization_matrix(matrix: &CscMatrix, diag_epsilon: f64) -> ValidationReport {
    let (nrows, ncols) = matrix.shape();
    let mut messages = Vec::new();

    let total_cells = f64::from(nrows.max(1)) * f64::from(ncols.max(1));
    let nnz = matrix.nnz();

    if nrows != ncols {
        messages.push("M must be square for direct solve".to_owned());
    }

    let mut empty_columns = 0_usize;
    let mut row_counts = vec![0_usize; usize::try_from(nrows).unwrap_or_default()];
    let mut near_zero_diagonal = 0_usize;

    for col in 0..usize::try_from(ncols).unwrap_or_default() {
        let start = usize::try_from(matrix.col_ptr[col]).unwrap_or_default();
        let end = usize::try_from(matrix.col_ptr[col + 1]).unwrap_or_default();
        if start == end {
            empty_columns += 1;
        }

        let mut found_diag = false;
        for idx in start..end {
            let row = usize::try_from(matrix.row_idx[idx]).unwrap_or_default();
            if row < row_counts.len() {
                row_counts[row] += 1;
            }

            if row == col {
                found_diag = true;
                if matrix.values[idx].abs() <= diag_epsilon {
                    near_zero_diagonal += 1;
                }
            }
        }

        if !found_diag {
            near_zero_diagonal += 1;
        }
    }

    let empty_rows = row_counts.iter().filter(|&&count| count == 0).count();

    if empty_columns > 0 {
        messages.push(format!("{empty_columns} empty columns detected"));
    }
    if empty_rows > 0 {
        messages.push(format!("{empty_rows} empty rows detected"));
    }
    if near_zero_diagonal > 0 {
        messages.push(format!(
            "{near_zero_diagonal} near-zero or missing diagonal entries"
        ));
    }

    let mut status = ValidationStatus::Ok;
    if !messages.is_empty() {
        status = ValidationStatus::WarningNearSingular;
    }
    if nrows <= 0 || ncols <= 0 || nrows != ncols {
        status = ValidationStatus::FailedInvalidStructure;
    }

    let nnz_f64 = u32::try_from(nnz).map_or(f64::from(u32::MAX), f64::from);

    ValidationReport {
        status,
        stats: MatrixStats {
            nrows,
            ncols,
            nnz,
            sparsity: 1.0 - (nnz_f64 / total_cells),
            empty_columns,
            empty_rows,
            near_zero_diagonal,
        },
        messages,
    }
}

#[cfg(test)]
mod tests {
    use suitesparse_ffi::CscMatrix;

    use super::{ValidationStatus, validate_factorization_matrix};

    #[test]
    fn warns_on_zero_diagonal() {
        let matrix =
            CscMatrix::new(2, 2, vec![0, 1, 2], vec![1, 0], vec![1.0, 2.0]).expect("matrix");
        let report = validate_factorization_matrix(&matrix, 1e-12);
        assert_eq!(report.status, ValidationStatus::WarningNearSingular);
        assert_eq!(report.stats.near_zero_diagonal, 2);
    }
}
