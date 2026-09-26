"""Collect completed benchmark summaries without averaging per-run P99 values."""

import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def main(results_dir, prefix):
    records = []
    for path in sorted(results_dir.glob(prefix + "*/summary.json")):
        summary = json.loads(path.read_text(encoding="utf-8"))
        metadata = json.loads((path.parent / "metadata.json").read_text(encoding="utf-8-sig"))
        records.append({
            "group": metadata["group"],
            "run_id": summary["run_id"],
            "valid": summary["state_conserved"] and summary["broker_coverage_complete"],
            "registered": summary["registered_events"],
            "publish_events_per_second": summary["overall_publish_events_per_second"],
            "steady_events_per_second": summary["steady_20_to_80_percent_events_per_second"],
            "p50_ms": summary["first_to_published_time_difference"]["p50_ms"],
            "p95_ms": summary["first_to_published_time_difference"]["p95_ms"],
            "p99_ms": summary["first_to_published_time_difference"]["p99_ms"],
            "mysql_cpu_pct_average": summary["mysql_cpu_pct_average"],
            "innodb_row_lock_waits_delta": summary["innodb_row_lock_waits_delta"],
            "sender_failure_rate": summary["sender_failure_rate"],
            "duplicate_message_rate": summary["duplicate_message_rate"],
        })
    if not records:
        raise SystemExit("No completed benchmark summaries found for prefix " + prefix)
    output = results_dir / (prefix + "-matrix.csv")
    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    groups = defaultdict(list)
    for record in records:
        groups[record["group"]].append(record)
    lines = ["# M5.3 matrix " + prefix, "", "Each row in the CSV is one run. P99 values remain per-run values.", ""]
    for group, group_records in sorted(groups.items()):
        valid = [record for record in group_records if record["valid"]]
        rates = [record["publish_events_per_second"] for record in valid
                 if record["publish_events_per_second"] is not None]
        median_rate = statistics.median(rates) if rates else None
        lines.append(f"- {group}: {len(valid)}/{len(group_records)} valid; median full-drain events/s: {median_rate}")
    lines.extend(["", "See each run directory for metadata, event samples, Broker identities and EXPLAIN plans."])
    (results_dir / (prefix + "-matrix.md")).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(output)
    invalid = [record["run_id"] for record in records if not record["valid"]]
    if invalid:
        raise SystemExit("Invalid benchmark runs: " + ", ".join(invalid))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: aggregate.py RESULTS_DIRECTORY PREFIX")
    main(Path(sys.argv[1]), sys.argv[2])
