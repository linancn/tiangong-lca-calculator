from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import psycopg
from psycopg.rows import dict_row
from scipy.sparse.linalg import splu

from .cli import S3Config, build_matrices, load_snapshot_payload
from .expected_cli import (
    DEFAULT_GWP_IMPACT_ID,
    fetch_snapshot_artifact_url,
    impact_index_of,
    load_snapshot_index,
    read_process_ids,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bw25-request-scope-validate",
        description=(
            "Compare a request-scoped snapshot against a broader full snapshot for "
            "LCIA parity, build timing, and Phase A readiness signals."
        ),
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("CONN"),
    )
    parser.add_argument("--full-snapshot-id", required=True)
    parser.add_argument("--scoped-snapshot-id", required=True)
    parser.add_argument("--process-ids-file", required=True)
    parser.add_argument("--impact-id", default=DEFAULT_GWP_IMPACT_ID)
    parser.add_argument("--abs-tol", type=float, default=1e-9)
    parser.add_argument("--rel-tol", type=float, default=1e-6)
    parser.add_argument("--fail-on-threshold", action="store_true")
    parser.add_argument(
        "--build-report-dir",
        default="reports/snapshot-coverage",
        help="Directory containing <snapshot_id>.json build reports from snapshot_builder",
    )
    parser.add_argument(
        "--report-dir",
        default="reports/request-scope-validation",
        help="Output directory for JSON/Markdown comparison reports",
    )
    parser.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT"))
    parser.add_argument("--s3-region", default=os.getenv("S3_REGION"))
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument("--s3-access-key-id", default=os.getenv("S3_ACCESS_KEY_ID"))
    parser.add_argument("--s3-secret-access-key", default=os.getenv("S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--s3-session-token", default=os.getenv("S3_SESSION_TOKEN"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("missing DB connection: provide --database-url or DATABASE_URL/CONN")

    process_ids = read_process_ids(args.process_ids_file)
    if not process_ids:
        raise SystemExit("process ids file contains no valid process_id")

    s3 = S3Config(
        endpoint=args.s3_endpoint,
        region=args.s3_region,
        bucket=args.s3_bucket,
        access_key_id=args.s3_access_key_id,
        secret_access_key=args.s3_secret_access_key,
        session_token=args.s3_session_token,
    )

    with psycopg.connect(args.database_url, row_factory=dict_row) as conn:
        full_snapshot = load_snapshot_bundle(conn, args.full_snapshot_id, args.impact_id, s3)
        scoped_snapshot = load_snapshot_bundle(conn, args.scoped_snapshot_id, args.impact_id, s3)

    missing = {
        "full_snapshot": [
            process_id for process_id in process_ids if process_id not in full_snapshot["process_idx_by_id"]
        ],
        "scoped_snapshot": [
            process_id
            for process_id in process_ids
            if process_id not in scoped_snapshot["process_idx_by_id"]
        ],
    }
    if missing["full_snapshot"] or missing["scoped_snapshot"]:
        raise SystemExit(
            "process ids missing from compared snapshots: "
            + json.dumps(missing, ensure_ascii=False)
        )

    full_expected = solve_expected_values(full_snapshot, process_ids)
    scoped_expected = solve_expected_values(scoped_snapshot, process_ids)
    comparisons = compare_expected_rows(
        process_ids, full_expected, scoped_expected, args.abs_tol, args.rel_tol
    )

    full_build_report = load_build_report(args.build_report_dir, args.full_snapshot_id)
    scoped_build_report = load_build_report(args.build_report_dir, args.scoped_snapshot_id)
    readiness = evaluate_phase_a_readiness(
        full_snapshot=full_snapshot,
        scoped_snapshot=scoped_snapshot,
        full_build_report=full_build_report,
        scoped_build_report=scoped_build_report,
        comparisons=comparisons,
    )

    generated_at = now_utc_iso()
    report = {
        "generated_at_utc": generated_at,
        "inputs": {
            "full_snapshot_id": args.full_snapshot_id,
            "scoped_snapshot_id": args.scoped_snapshot_id,
            "impact_id": args.impact_id,
            "process_ids_file": args.process_ids_file,
            "process_count": len(process_ids),
            "abs_tol": args.abs_tol,
            "rel_tol": args.rel_tol,
        },
        "snapshot_summary": {
            "full": snapshot_summary(full_snapshot, full_build_report),
            "scoped": snapshot_summary(scoped_snapshot, scoped_build_report),
        },
        "parity": {
            "metrics": comparisons["metrics"],
            "rows": comparisons["rows"],
        },
        "readiness": readiness,
        "gaps": readiness["gaps"],
    }

    out_dir = Path(args.report_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_name = (
        f"request-scope-{args.scoped_snapshot_id}-vs-{args.full_snapshot_id}"
    )
    json_path = out_dir / f"{base_name}.json"
    md_path = out_dir / f"{base_name}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")

    print(f"[done] request-scope validation report: {json_path}")
    print(
        "[parity] expected_abs_inf={} expected_rel_inf={} verdict={}".format(
            comparisons["metrics"]["expected"]["abs_inf"],
            comparisons["metrics"]["expected"]["rel_inf"],
            readiness["parity_verdict"],
        )
    )
    print(
        "[scope] full_process_count={} scoped_process_count={}".format(
            full_snapshot["process_count"], scoped_snapshot["process_count"]
        )
    )
    if readiness["failures"] and args.fail_on_threshold:
        raise SystemExit("request-scope validation failed readiness checks")


def load_snapshot_bundle(
    conn: psycopg.Connection[Any],
    snapshot_id: str,
    impact_id: str,
    s3: S3Config,
) -> dict[str, Any]:
    payload = load_snapshot_payload(conn, snapshot_id, s3)
    artifact_url = fetch_snapshot_artifact_url(conn, snapshot_id)
    snapshot_index = load_snapshot_index(snapshot_id, artifact_url, s3)
    impact_idx = impact_index_of(snapshot_index, impact_id)
    if impact_idx is None:
        raise SystemExit(f"impact_id not found in snapshot {snapshot_id}: {impact_id}")
    m, b, c = build_matrices(payload)
    process_idx_by_id = {
        str(item["process_id"]): int(item["process_index"])
        for item in snapshot_index.get("process_map", [])
        if isinstance(item, dict)
    }
    solver = splu(m.tocsc())
    d = (c.getrow(int(impact_idx)) @ b).toarray().ravel()
    return {
        "snapshot_id": snapshot_id,
        "artifact_url": artifact_url,
        "payload": payload,
        "snapshot_index": snapshot_index,
        "impact_index": int(impact_idx),
        "process_idx_by_id": process_idx_by_id,
        "process_count": int(payload.get("process_count", 0)),
        "flow_count": int(payload.get("flow_count", 0)),
        "impact_count": int(payload.get("impact_count", 0)),
        "solver": solver,
        "demand_lcia_vector": d,
    }


def solve_expected_values(snapshot: dict[str, Any], process_ids: list[str]) -> dict[str, dict[str, float]]:
    solver = snapshot["solver"]
    d = snapshot["demand_lcia_vector"]
    n = int(snapshot["process_count"])
    results: dict[str, dict[str, float]] = {}
    for process_id in process_ids:
        process_idx = int(snapshot["process_idx_by_id"][process_id])
        rhs = np.zeros(n, dtype=np.float64)
        rhs[process_idx] = 1.0
        x = solver.solve(rhs)
        expected = float(np.dot(d, x))
        direct = float(d[process_idx])
        results[process_id] = {
            "expected_value": expected,
            "direct_value": direct,
            "indirect_value": expected - direct,
        }
    return results


def compare_expected_rows(
    process_ids: list[str],
    full_expected: dict[str, dict[str, float]],
    scoped_expected: dict[str, dict[str, float]],
    abs_tol: float,
    rel_tol: float,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    metric_values: dict[str, list[tuple[float, float]]] = {
        "expected": [],
        "direct": [],
        "indirect": [],
    }
    for process_id in process_ids:
        row: dict[str, Any] = {"process_id": process_id}
        for key, left_name, right_name in [
            ("expected", "expected_value", "expected_value"),
            ("direct", "direct_value", "direct_value"),
            ("indirect", "indirect_value", "indirect_value"),
        ]:
            full_value = float(full_expected[process_id][left_name])
            scoped_value = float(scoped_expected[process_id][right_name])
            abs_diff = abs(scoped_value - full_value)
            rel_diff = abs_diff / max(abs(full_value), abs(scoped_value), 1e-12)
            row[f"{key}_full"] = full_value
            row[f"{key}_scoped"] = scoped_value
            row[f"{key}_abs_diff"] = abs_diff
            row[f"{key}_rel_diff"] = rel_diff
            metric_values[key].append((abs_diff, rel_diff))
        rows.append(row)

    metrics: dict[str, Any] = {}
    failures: list[str] = []
    for key, values in metric_values.items():
        abs_inf = max((item[0] for item in values), default=0.0)
        rel_inf = max((item[1] for item in values), default=0.0)
        passed = abs_inf <= abs_tol or rel_inf <= rel_tol
        metrics[key] = {
            "abs_inf": abs_inf,
            "rel_inf": rel_inf,
            "pass_threshold": passed,
        }
        if not passed:
            failures.append(f"{key} parity exceeded threshold")

    return {"rows": rows, "metrics": metrics, "failures": failures}


def load_build_report(build_report_dir: str, snapshot_id: str) -> dict[str, Any] | None:
    path = Path(build_report_dir) / f"{snapshot_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def snapshot_summary(snapshot: dict[str, Any], build_report: dict[str, Any] | None) -> dict[str, Any]:
    summary = {
        "snapshot_id": snapshot["snapshot_id"],
        "artifact_url": snapshot["artifact_url"],
        "process_count": snapshot["process_count"],
        "flow_count": snapshot["flow_count"],
        "impact_count": snapshot["impact_count"],
    }
    if build_report:
        resolved_scope = build_report.get("resolved_scope") or {}
        build_timing = build_report.get("build_timing_sec") or {}
        summary["resolved_scope"] = {
            "selection_mode": resolved_scope.get("selection_mode"),
            "public_process_count": resolved_scope.get("public_process_count"),
            "private_process_count": resolved_scope.get("private_process_count"),
            "resolved_process_count": len(resolved_scope.get("processes") or []),
        }
        summary["build_timing_sec"] = {
            "total_sec": build_timing.get("total_sec"),
            "resolve_process_selection_sec": build_timing.get("resolve_process_selection_sec"),
            "compile_scope_graph_sec": build_timing.get("compile_scope_graph_sec"),
            "assemble_sparse_payload_sec": build_timing.get("assemble_sparse_payload_sec"),
            "build_sparse_payload_sec": build_timing.get("build_sparse_payload_sec"),
        }
    return summary


def evaluate_phase_a_readiness(
    *,
    full_snapshot: dict[str, Any],
    scoped_snapshot: dict[str, Any],
    full_build_report: dict[str, Any] | None,
    scoped_build_report: dict[str, Any] | None,
    comparisons: dict[str, Any],
) -> dict[str, Any]:
    failures = list(comparisons["failures"])
    gaps: list[str] = []

    scoped_process_smaller = scoped_snapshot["process_count"] <= full_snapshot["process_count"]
    if not scoped_process_smaller:
        failures.append("scoped snapshot is unexpectedly larger than full snapshot")

    scoped_scope = (scoped_build_report or {}).get("resolved_scope") or {}
    has_partition_counts = (
        scoped_scope.get("public_process_count") is not None
        and scoped_scope.get("private_process_count") is not None
    )
    if not has_partition_counts:
        failures.append("scoped build report is missing public/private partition counts")

    build_timing = (scoped_build_report or {}).get("build_timing_sec") or {}
    required_stage_timing_fields = [
        "resolve_process_selection_sec",
        "compile_scope_graph_sec",
        "assemble_sparse_payload_sec",
    ]
    has_stage_timings = all(build_timing.get(field) is not None for field in required_stage_timing_fields)
    if not has_stage_timings:
        failures.append("scoped build report is missing stage-level request-scoped timing fields")

    if not full_build_report:
        gaps.append("Full snapshot build report JSON was not found; timing comparison is partial.")
    if not scoped_build_report:
        gaps.append("Scoped snapshot build report JSON was not found; readiness depends only on artifact parity.")

    gaps.append(
        "Runtime snapshot artifacts still do not persist compiled coupling-edge rows directly; "
        "cross-partition coupling visibility is currently asserted by repo-level fixture tests and compiled-graph boundaries."
    )

    return {
        "parity_verdict": "pass" if not comparisons["failures"] else "fail",
        "phase_a_ready_for_default": not failures,
        "phase_a_ready_for_block_solve_boundary": has_partition_counts and has_stage_timings,
        "scoped_process_count_le_full": scoped_process_smaller,
        "has_public_private_partition_counts": has_partition_counts,
        "has_stage_timings": has_stage_timings,
        "failures": failures,
        "gaps": gaps,
    }


def render_markdown(report: dict[str, Any]) -> str:
    inputs = report["inputs"]
    full = report["snapshot_summary"]["full"]
    scoped = report["snapshot_summary"]["scoped"]
    readiness = report["readiness"]
    parity = report["parity"]["metrics"]

    md = "# Request-Scoped Validation Report\n\n"
    md += f"- generated_at_utc: `{report['generated_at_utc']}`\n"
    md += f"- full_snapshot_id: `{inputs['full_snapshot_id']}`\n"
    md += f"- scoped_snapshot_id: `{inputs['scoped_snapshot_id']}`\n"
    md += f"- process_ids_file: `{inputs['process_ids_file']}`\n"
    md += f"- process_count: `{inputs['process_count']}`\n"
    md += f"- impact_id: `{inputs['impact_id']}`\n\n"

    md += "## Snapshot Summary\n\n"
    md += f"- full_process_count: `{full['process_count']}`\n"
    md += f"- scoped_process_count: `{scoped['process_count']}`\n"
    if full.get("resolved_scope"):
        md += (
            f"- full_selection_mode: `{full['resolved_scope'].get('selection_mode', 'unknown')}`\n"
        )
    if scoped.get("resolved_scope"):
        md += (
            f"- scoped_selection_mode: `{scoped['resolved_scope'].get('selection_mode', 'unknown')}`\n"
        )
        md += (
            f"- scoped_public_process_count: `{scoped['resolved_scope'].get('public_process_count')}`\n"
        )
        md += (
            f"- scoped_private_process_count: `{scoped['resolved_scope'].get('private_process_count')}`\n"
        )
    md += "\n## Parity Metrics\n\n"
    for key in ["expected", "direct", "indirect"]:
        md += f"- {key}_abs_inf: `{parity[key]['abs_inf']}`\n"
        md += f"- {key}_rel_inf: `{parity[key]['rel_inf']}`\n"
        md += f"- {key}_pass_threshold: `{parity[key]['pass_threshold']}`\n"
    md += "\n## Readiness\n\n"
    md += f"- parity_verdict: `{readiness['parity_verdict']}`\n"
    md += f"- phase_a_ready_for_default: `{readiness['phase_a_ready_for_default']}`\n"
    md += (
        f"- phase_a_ready_for_block_solve_boundary: `{readiness['phase_a_ready_for_block_solve_boundary']}`\n"
    )
    md += f"- has_public_private_partition_counts: `{readiness['has_public_private_partition_counts']}`\n"
    md += f"- has_stage_timings: `{readiness['has_stage_timings']}`\n"
    if readiness["failures"]:
        md += "\n## Failures\n\n"
        for failure in readiness["failures"]:
            md += f"- {failure}\n"
    if readiness["gaps"]:
        md += "\n## Gaps\n\n"
        for gap in readiness["gaps"]:
            md += f"- {gap}\n"
    return md


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    main()
