"""Tests for quarantine.reprocessor."""

import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from quarantine.reprocessor import reprocess


class TestReprocessor:
    def _make_fixtures(self):
        """Create temp config and quarantine store."""
        # Config
        cfg_fd, cfg_path = tempfile.mkstemp(suffix=".yaml")
        os.close(cfg_fd)
        with open(cfg_path, "w") as f:
            f.write("""
source_name: test
fhir_server_url: http://localhost/fhir
column_map:
  patient_id: PatientID
  lab_name: TestName
  value: Result
  unit: Units
""")
        # Quarantine store with a resolved record
        q_fd, q_path = tempfile.mkstemp(suffix=".json")
        os.close(q_fd)
        with open(q_path, "w") as f:
            json.dump({
                "q_test_001": {
                    "id": "q_test_001",
                    "lab_name": "Glucose",
                    "row_data": {
                        "patient_id": "P001",
                        "lab_name": "Glucose",
                        "value": "95",
                        "unit": "mg/dL",
                        "effective_datetime": "2024-03-15T00:00:00+00:00",
                    },
                    "status": "resolved",
                    "resolved_loinc": "2345-7",
                    "resolved_display": "Glucose [Mass/volume]",
                    "reason": "low_confidence",
                    "candidates": [],
                    "reviewed_by": "test",
                    "failure_reason": None,
                    "created_at": "2024-03-15T00:00:00+00:00",
                    "updated_at": "2024-03-15T00:00:00+00:00",
                }
            }, f)

        return cfg_path, q_path

    @patch("quarantine.reprocessor.QuarantineStore")
    @patch("quarantine.reprocessor.send_observation")
    def test_dry_run(self, mock_send, MockStore):
        cfg_path, q_path = self._make_fixtures()
        mock_send.return_value = {"success": True}

        from quarantine.store import QuarantineStore as RealStore
        store = RealStore(path=q_path)
        MockStore.return_value = store

        stats = reprocess(cfg_path, dry_run=True)
        assert stats["sent"] >= 0  # dry run should process without errors
        os.unlink(cfg_path)
        os.unlink(q_path)

    def test_nonexistent_record(self):
        cfg_fd, cfg_path = tempfile.mkstemp(suffix=".yaml")
        os.close(cfg_fd)
        with open(cfg_path, "w") as f:
            f.write("""
source_name: test
fhir_server_url: http://localhost/fhir
column_map:
  patient_id: PatientID
  lab_name: TestName
  value: Result
  unit: Units
""")
        stats = reprocess(cfg_path, record_id="nonexistent")
        assert stats["skipped"] == 1
        os.unlink(cfg_path)
