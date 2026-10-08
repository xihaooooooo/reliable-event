"""Turn one benchmark run's raw files into a reproducible summary (stdlib only)."""

import csv
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path


def rows(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def percentile(sorted_values, fraction):
    if not sorted_values:
        return None
    position = (len(sorted_values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (position - lower)


def distribution(values):
    values = sorted(values)
    return {
        "samples": len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
    }


def numeric(row, key):
    try:
        return float(row[key])
    except (ValueError, TypeError, KeyError):
        return None


def main(run_dir):
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8-sig"))
    events = rows(run_dir / "events.csv")
    messages = rows(run_dir / "messages.csv")
    samples = rows(run_dir / "samples.csv")
    registrations = rows(run_dir / "registration.csv")
    faults = rows(run_dir / "faults.csv") if (run_dir / "faults.csv").exists() else []
    expected = int(metadata["count"])
    sample_epochs = [int(value) for sample in samples if (value := numeric(sample, "epoch_ms")) is not None]
    sample_window = {
        "sample_count": len(sample_epochs),
        "start_epoch_ms": sample_epochs[0] if sample_epochs else None,
        "end_epoch_ms": sample_epochs[-1] if sample_epochs else None,
        "duration_ms": sample_epochs[-1] - sample_epochs[0] if len(sample_epochs) > 1 else None,
        "nominal_period_ms": metadata.get("m94SamplePeriodNominalMs"),
    }

    statuses = Counter(int(event["status"]) for event in events)
    lags = []
    negative_lags = 0
    for event in events:
        if event["status"] == "2" and event["lag_ms"]:
            lag = float(event["lag_ms"])
            if lag < 0:
                negative_lags += 1
            else:
                lags.append(lag)

    identities = Counter(message["event_id"] for message in messages if message["event_id"])
    extra_messages = sum(max(0, count - 1) for count in identities.values())
    observed_identities = {(message["event_id"], message["event_key"]) for message in messages}
    published_identities = {(event["event_id"], event["event_key"])
                            for event in events if event["status"] == "2"}
    broker_coverage_complete = published_identities <= observed_identities

    attempts = Counter()
    failure_types = Counter()
    send_timers = {}
    lag_timers = []
    for path in run_dir.glob("publisher-*-meters.csv"):
        latest = {}
        observed_max = {}
        for row in rows(path):
            key = (row["meter"], row["outcome"])
            latest[key] = row
            if row["meter"] in {"publish.duration", "publish.lag"}:
                observed_max[key] = max(observed_max.get(key, 0.0), float(row["max_ms"]))
        for (meter, outcome), row in latest.items():
            if meter == "publish.duration":
                aggregate = send_timers.setdefault(outcome, {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
                aggregate["count"] += int(row["count"])
                aggregate["total_ms"] += float(row["total_ms"])
                aggregate["max_ms"] = max(aggregate["max_ms"], observed_max[(meter, outcome)])
            elif meter == "publish.lag":
                lag_timers.append((row, observed_max[(meter, outcome)]))
    for aggregate in send_timers.values():
        aggregate["mean_ms"] = (aggregate["total_ms"] / aggregate["count"]
                                if aggregate["count"] else None)
    lag_timer = {
        "count": sum(int(row["count"]) for row, _ in lag_timers),
        "total_ms": sum(float(row["total_ms"]) for row, _ in lag_timers),
        "max_ms": max((observed for _, observed in lag_timers), default=None),
    }
    lag_timer["mean_ms"] = (lag_timer["total_ms"] / lag_timer["count"]
                            if lag_timer["count"] else None)
    load_duration_ns = None
    for path in run_dir.glob("publisher-*.stdout.log"):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "event=reliable_event.send.succeeded" in line:
                attempts["success"] += 1
            elif "event=reliable_event.publish.failed" in line:
                attempts["failure"] += 1
                match = re.search(r"failureType=(RETRYABLE|NON_RETRYABLE|RESULT_UNKNOWN)", line)
                if match:
                    failure_types[match.group(1)] += 1
    load_log = (run_dir / "load.stdout.log").read_text(encoding="utf-8", errors="replace")
    match = re.search(r"BENCHMARK_LOAD_RUN_ID=\S+ COUNT=\d+ DURATION_NS=(\d+)", load_log)
    if match:
        load_duration_ns = int(match.group(1))

    cpu = [numeric(sample, "mysql_cpu_pct") for sample in samples]
    cpu = [value for value in cpu if value is not None]
    connections = [numeric(sample, "threads_connected") for sample in samples]
    connections = [value for value in connections if value is not None]
    active_connections = [numeric(sample, "threads_running") for sample in samples]
    active_connections = [value for value in active_connections if value is not None]
    publisher_memory = []
    for sample in samples:
        working_set = numeric(sample, "publisher_working_set_bytes")
        if working_set is None:  # Read M5.3 evidence that used the earlier CSV field name.
            working_set = numeric(sample, "publisher_memory_bytes")
        publisher_memory.append(working_set)
    publisher_memory = [value for value in publisher_memory if value is not None]
    publisher_private_bytes = [numeric(sample, "publisher_private_bytes") for sample in samples]
    publisher_private_bytes = [value for value in publisher_private_bytes if value is not None]
    publisher_thread_count = [numeric(sample, "publisher_thread_count") for sample in samples]
    publisher_thread_count = [value for value in publisher_thread_count if value is not None]
    publisher_handle_count = [numeric(sample, "publisher_handle_count") for sample in samples]
    publisher_handle_count = [value for value in publisher_handle_count if value is not None]
    receiver_memory = [numeric(sample, "receiver_working_set_bytes") for sample in samples]
    receiver_memory = [value for value in receiver_memory if value is not None]
    receiver_private_bytes = [numeric(sample, "receiver_private_bytes") for sample in samples]
    receiver_private_bytes = [value for value in receiver_private_bytes if value is not None]
    receiver_thread_count = [numeric(sample, "receiver_thread_count") for sample in samples]
    receiver_thread_count = [value for value in receiver_thread_count if value is not None]
    receiver_handle_count = [numeric(sample, "receiver_handle_count") for sample in samples]
    receiver_handle_count = [value for value in receiver_handle_count if value is not None]
    lock_waits = [numeric(sample, "innodb_row_lock_waits") for sample in samples]
    lock_time = [numeric(sample, "innodb_row_lock_time_ms") for sample in samples]
    published_times = sorted(int(event["published_epoch_ms"]) for event in events
                             if event["status"] == "2" and event["published_epoch_ms"] != "-1")
    first_commit = min((int(row["commit_return_epoch_ms"]) for row in registrations), default=None)
    overall_publish_rate = None
    if len(published_times) == expected and first_commit is not None and published_times[-1] > first_commit:
        overall_publish_rate = expected / ((published_times[-1] - first_commit) / 1000)
    steady_publish_rate = None
    if len(published_times) == expected:
        low_index = math.floor((expected - 1) * 0.2)
        high_index = math.ceil((expected - 1) * 0.8)
        span_ms = published_times[high_index] - published_times[low_index]
        if span_ms > 0:
            steady_publish_rate = (high_index - low_index) / (span_ms / 1000)

    summary = {
        "run_id": metadata["runId"],
        "expected_events": expected,
        "registered_batches": len(registrations),
        "registered_events": sum(int(row["count"]) for row in registrations),
        "registration_duration_seconds": load_duration_ns / 1e9 if load_duration_ns else None,
        "registration_events_per_second": expected / (load_duration_ns / 1e9) if load_duration_ns else None,
        "outbox_rows": len(events),
        "resource_sample_window": sample_window,
        "status_counts": {str(code): statuses[code] for code in range(5)},
        "state_conserved": len(events) == expected and statuses[2] == expected,
        "first_to_published_time_difference": distribution(lags),
        "negative_lag_samples": negative_lags,
        "overall_publish_events_per_second": overall_publish_rate,
        "steady_20_to_80_percent_events_per_second": steady_publish_rate,
        "sender_attempts": dict(attempts),
        "send_duration_by_outcome": send_timers,
        "first_available_to_successful_receipt_timer": lag_timer,
        "sender_failure_types": dict(failure_types),
        "sender_failure_rate": attempts["failure"] / sum(attempts.values()) if attempts else None,
        "broker_messages_observed": len(messages),
        "broker_coverage_complete": broker_coverage_complete,
        "duplicate_event_identities": sum(1 for count in identities.values() if count > 1),
        "extra_broker_messages": extra_messages,
        "duplicate_message_rate": extra_messages / len(messages) if messages and broker_coverage_complete else None,
        "mysql_cpu_pct_average": sum(cpu) / len(cpu) if cpu else None,
        "mysql_cpu_pct_maximum": max(cpu) if cpu else None,
        "mysql_connected_threads_maximum": max(connections) if connections else None,
        "mysql_running_threads_maximum": max(active_connections) if active_connections else None,
        "publisher_memory_bytes_maximum": max(publisher_memory) if publisher_memory else None,
        "publisher_private_bytes_maximum_sampled": max(publisher_private_bytes) if publisher_private_bytes else None,
        "publisher_thread_count_maximum_sampled": max(publisher_thread_count) if publisher_thread_count else None,
        "publisher_handle_count_maximum_sampled": max(publisher_handle_count) if publisher_handle_count else None,
        "receiver_memory_bytes_maximum_sampled": max(receiver_memory) if receiver_memory else None,
        "receiver_private_bytes_maximum_sampled": max(receiver_private_bytes) if receiver_private_bytes else None,
        "receiver_thread_count_maximum_sampled": max(receiver_thread_count) if receiver_thread_count else None,
        "receiver_handle_count_maximum_sampled": max(receiver_handle_count) if receiver_handle_count else None,
        "mysql_block_io_first_last": [samples[0].get("mysql_block_io"), samples[-1].get("mysql_block_io")] if samples else None,
        "broker_block_io_first_last": [samples[0].get("broker_block_io"), samples[-1].get("broker_block_io")] if samples else None,
        "innodb_row_lock_waits_delta": lock_waits[-1] - lock_waits[0] if len(lock_waits) >= 2 and None not in (lock_waits[0], lock_waits[-1]) else None,
        "innodb_row_lock_time_ms_delta": lock_time[-1] - lock_time[0] if len(lock_time) >= 2 and None not in (lock_time[0], lock_time[-1]) else None,
        "fault_timeline": faults,
        "raw_files": [path.name for path in sorted(run_dir.iterdir())
                      if path.is_file() and path.name not in {"summary.json", "report.md"}],
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [
        f"# Benchmark {summary['run_id']}",
        "",
        "All rates and latency values below are derived from the adjacent raw files."
        " The PUBLISHED timestamp precedes the database commit and is not a precise commit time.",
        "",
        f"- Registered: {summary['registered_events']} / {expected}; Outbox rows: {summary['outbox_rows']}",
        f"- Resource sampling window (actual first/last epoch ms, duration ms, samples, nominal period ms): "
        f"{sample_window['start_epoch_ms']} / {sample_window['end_epoch_ms']} / "
        f"{sample_window['duration_ms']} / {sample_window['sample_count']} / "
        f"{sample_window['nominal_period_ms']}",
        f"- Final states (0/1/2/3/4): {summary['status_counts']}; conserved: {summary['state_conserved']}",
        f"- Registration events/s: {summary['registration_events_per_second']}",
        f"- Publication events/s, first batch commit return to last PUBLISHED timestamp: {overall_publish_rate};"
        f" steady 20–80% event timestamps: {steady_publish_rate}",
        f"- First availability to PUBLISHED timestamp difference (ms): {summary['first_to_published_time_difference']}",
        f"- Sender attempts: {dict(attempts)}; failure types: {dict(failure_types)}; failure rate: {summary['sender_failure_rate']}",
        f"- Sender duration by outcome (count/total/mean/max ms): {send_timers}",
        f"- First availability to successful receipt timer: {lag_timer}",
        f"- Broker messages: {len(messages)}; complete coverage: {broker_coverage_complete};"
        f" duplicate identities: {summary['duplicate_event_identities']}; duplicate rate: {summary['duplicate_message_rate']}",
        f"- MySQL CPU avg/max (% of one CPU): {summary['mysql_cpu_pct_average']} / {summary['mysql_cpu_pct_maximum']};"
        f" max connected/running threads: {summary['mysql_connected_threads_maximum']} /"
        f" {summary['mysql_running_threads_maximum']}",
        f"- Publisher maximum sampled working set/private bytes: {summary['publisher_memory_bytes_maximum']} / "
        f"{summary['publisher_private_bytes_maximum_sampled']}; maximum sampled threads/handles: "
        f"{summary['publisher_thread_count_maximum_sampled']} / {summary['publisher_handle_count_maximum_sampled']};"
        f" receiver working set/private bytes: {summary['receiver_memory_bytes_maximum_sampled']} / "
        f"{summary['receiver_private_bytes_maximum_sampled']}; receiver threads/handles: "
        f"{summary['receiver_thread_count_maximum_sampled']} / {summary['receiver_handle_count_maximum_sampled']};"
        f" MySQL block I/O first/last: {summary['mysql_block_io_first_last']};"
        f" Broker block I/O first/last: {summary['broker_block_io_first_last']}",
        f"- InnoDB row lock waits/time deltas: {summary['innodb_row_lock_waits_delta']} / {summary['innodb_row_lock_time_ms_delta']} ms",
        f"- Fault timeline (epoch ms): {faults}",
        "",
        "See metadata.json for configuration and environment, samples.csv for time series,"
        " events.csv for event-level data, messages.csv for Broker identities,"
        " and explain-*.txt for query plans.",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(run_dir / "report.md")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: report.py RUN_DIRECTORY")
    main(Path(sys.argv[1]))
