import csv
import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import report


def write_csv(path, fieldnames, values):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(fieldnames)
        writer.writerows(values)


class ReportContractTest(unittest.TestCase):
    def create_run(self, directory, include_second_message):
        directory.mkdir()
        (directory / "metadata.json").write_text(
            json.dumps({"runId": "contract", "count": 2, "m94SamplePeriodNominalMs": 1000}),
            encoding="utf-8")
        write_csv(directory / "events.csv",
                  ["event_id", "event_key", "status", "attempt_count", "first_available_epoch_ms",
                   "published_epoch_ms", "lag_ms"],
                  [[1, "contract-0", 2, 2, 1000, 2000, 1000],
                   [2, "contract-1", 2, 1, 1000, 3000, 2000]])
        messages = [[2000, 1, "contract-0", "broker-a"], [2100, 1, "contract-0", "broker-b"]]
        if include_second_message:
            messages.append([3000, 2, "contract-1", "broker-c"])
        write_csv(directory / "messages.csv",
                  ["received_epoch_ms", "event_id", "event_key", "broker_message_id"], messages)
        write_csv(directory / "samples.csv",
                  ["epoch_ms", "published", "mysql_cpu_pct", "publisher_working_set_bytes",
                   "publisher_private_bytes", "publisher_thread_count", "publisher_handle_count",
                   "receiver_working_set_bytes", "receiver_private_bytes", "receiver_thread_count",
                   "receiver_handle_count",
                   "threads_connected", "innodb_row_lock_waits", "innodb_row_lock_time_ms"],
                  [[1000, 0, 10, 1000, 2000, 8, 12, 400, 600, 10, 16, 2, 4, 9],
                   [3000, 2, 20, 1500, 2500, 9, 14, 500, 700, 11, 17, 3, 5, 11]])
        write_csv(directory / "registration.csv",
                  ["start_sequence", "count", "commit_return_epoch_ms", "batch_duration_ns"],
                  [[0, 2, 1500, 100000000]])
        (directory / "publisher-0.stdout.log").write_text(
            "event=reliable_event.publish.failed failureType=RESULT_UNKNOWN\n"
            "event=reliable_event.send.succeeded\n"
            "event=reliable_event.send.succeeded\n", encoding="utf-8")
        (directory / "load.stdout.log").write_text(
            "BENCHMARK_LOAD_RUN_ID=contract COUNT=2 DURATION_NS=100000000\n", encoding="utf-8")
        write_csv(directory / "publisher-0-meters.csv",
                  ["epoch_ms", "meter", "outcome", "count", "total_ms", "max_ms", "value"],
                  [[2000, "publish.duration", "success", 1, 20, 20, ""],
                   [2000, "publish.duration", "unknown", 1, 5, 5, ""],
                   [2000, "publish.lag", "", 1, 2000, 2000, ""],
                   [3000, "publish.duration", "success", 2, 30, 0, ""],
                   [3000, "publish.duration", "unknown", 1, 5, 0, ""],
                   [3000, "publish.lag", "", 2, 3000, 0, ""]])

    def test_expected_duplicate_keeps_one_business_identity(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "run"
            self.create_run(directory, True)
            report.main(directory)
            summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
            self.assertTrue(summary["state_conserved"])
            self.assertTrue(summary["broker_coverage_complete"])
            self.assertEqual(summary["duplicate_event_identities"], 1)
            self.assertEqual(summary["extra_broker_messages"], 1)
            self.assertAlmostEqual(summary["duplicate_message_rate"], 1 / 3)
            self.assertEqual(summary["sender_failure_types"]["RESULT_UNKNOWN"], 1)
            self.assertEqual(summary["send_duration_by_outcome"]["success"]["mean_ms"], 15)
            self.assertEqual(summary["send_duration_by_outcome"]["unknown"]["max_ms"], 5)
            self.assertEqual(summary["first_available_to_successful_receipt_timer"]["count"], 2)
            self.assertEqual(summary["first_available_to_successful_receipt_timer"]["max_ms"], 2000)
            self.assertEqual(summary["publisher_memory_bytes_maximum"], 1500)
            self.assertEqual(summary["publisher_private_bytes_maximum_sampled"], 2500)
            self.assertEqual(summary["publisher_thread_count_maximum_sampled"], 9)
            self.assertEqual(summary["publisher_handle_count_maximum_sampled"], 14)
            self.assertEqual(summary["receiver_memory_bytes_maximum_sampled"], 500)
            self.assertEqual(summary["receiver_private_bytes_maximum_sampled"], 700)
            self.assertEqual(summary["receiver_thread_count_maximum_sampled"], 11)
            self.assertEqual(summary["receiver_handle_count_maximum_sampled"], 17)
            self.assertEqual(summary["resource_sample_window"], {
                "sample_count": 2,
                "start_epoch_ms": 1000,
                "end_epoch_ms": 3000,
                "duration_ms": 2000,
                "nominal_period_ms": 1000,
            })

    def test_incomplete_broker_probe_does_not_claim_zero_duplicates(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "run"
            self.create_run(directory, False)
            report.main(directory)
            summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
            self.assertFalse(summary["broker_coverage_complete"])
            self.assertIsNone(summary["duplicate_message_rate"])

    def test_legacy_working_set_column_remains_readable(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "run"
            self.create_run(directory, True)
            write_csv(directory / "samples.csv", ["epoch_ms", "publisher_memory_bytes"],
                      [[1000, 1000], [2000, 3000]])
            report.main(directory)
            summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["publisher_memory_bytes_maximum"], 3000)

    def test_broker_coverage_requires_matching_stable_id_and_key(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "run"
            self.create_run(directory, True)
            write_csv(directory / "messages.csv",
                      ["received_epoch_ms", "event_id", "event_key", "broker_message_id"],
                      [[2000, 1, "contract-0", "broker-a"],
                       [2100, 1, "contract-0", "broker-b"],
                       [3000, 999, "contract-1", "broker-c"]])
            report.main(directory)
            summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
            self.assertFalse(summary["broker_coverage_complete"])
            self.assertIsNone(summary["duplicate_message_rate"])


if __name__ == "__main__":
    unittest.main()
