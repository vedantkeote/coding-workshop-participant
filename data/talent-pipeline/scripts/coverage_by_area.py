"""Coverage per area vs the targets in docs/data-engineer.md ("Test Coverage Goals").

coverage.py reports per file, but job.py is one file by necessity (deployment ships only
job.py), so this maps each function to a guide area and sums line coverage per area.

Usage (from data/talent-pipeline/):
    .venv/bin/pytest --cov=job --cov-report=json:.pytest_cache/coverage.json -q
    .venv/bin/python scripts/coverage_by_area.py .pytest_cache/coverage.json
"""

import ast
import json
import sys
from pathlib import Path

# area -> (target %, functions/classes that implement it)
AREAS = {
    "Core transformation functions": (80, {
        "_bronze_postgres", "plan_learning_copy", "_bronze_learning", "coerce", "parse_jsonl",
        "apply_field_aliases", "build_dim_employee", "_silver_tables", "_silver_learning_events",
        "_table_quarantine", "write_silver", "write_quarantine", "_publish", "read_bronze_table", "read_silver",
        "insert_before_lineage",
        "Lake", "SourceDatabase",
    }),
    "Schema and validation rules": (90, {
        "validate", "Rule", "Contract", "one_of", "between", "matches", "not_after", "unique_together",
        "references", "source_contracts", "quarantine_drifted_files", "check_gold", "Settings", "_parse_run_date",
        "flag_before_hire", "report_before_hire",
    }),
    "Error-handling and retry paths": (85, {
        "with_retry", "RunContext", "preflight", "write_manifest", "_quarantine_examples",
    }),
    "Critical business metric calculations": (95, {
        "flag", "period_start", "period_end", "gold_performance_history", "gold_skill_gaps", "gold_promotion_readiness",
        "gold_learning_summary", "gold_attrition_risk", "gold_team_outcomes", "gold_skill_distribution",
        "compute_gold",
    }),
}


def function_lines(source: str) -> dict[str, set[int]]:
    """Top-level function/class name -> its line numbers."""
    tree = ast.parse(source)
    return {node.name: set(range(node.lineno, node.end_lineno + 1))
            for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))}


def main(report_path: str) -> int:
    report = json.loads(Path(report_path).read_text())
    job_file = next(f for f in report["files"] if f.endswith("job.py"))
    data = report["files"][job_file]
    executed, missing = set(data["executed_lines"]), set(data["missing_lines"])
    spans = function_lines(Path(job_file).read_text() if Path(job_file).exists()
                           else (Path(report_path).parent / "job.py").read_text())

    unknown = {name for _, names in AREAS.values() for name in names} - spans.keys()
    if unknown:
        print(f"unknown names in AREAS: {sorted(unknown)}")
        return 2

    failed = False
    print(f"{'Area':42} {'Target':>7} {'Actual':>7} {'Lines':>11}  Result")
    for area, (target, names) in AREAS.items():
        lines = set().union(*(spans[n] for n in names))
        hit, miss = len(lines & executed), len(lines & missing)
        pct = 100 * hit / (hit + miss) if hit + miss else 100.0
        ok = pct >= target
        failed |= not ok
        print(f"{area:42} {target:>6}% {pct:>6.1f}% {hit:>5}/{hit + miss:<5}  {'PASS' if ok else 'BELOW TARGET'}")
    total = data["summary"]["percent_covered"]
    print(f"{'Whole job.py':42} {'':>7} {total:>6.1f}%")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else ".pytest_cache/coverage.json"))
