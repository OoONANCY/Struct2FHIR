"""Tests for quarantine.store."""

import json
import os
import tempfile
import pytest

from quarantine.store import QuarantineStore


class TestQuarantineStore:
    def _make_store(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        with open(path, "w") as f:
            json.dump({}, f)
        return QuarantineStore(path=path), path

    def test_add_record(self):
        store, path = self._make_store()
        qid = store.add("Glucose", {"patient_id": "P001", "value": "95"})
        assert qid.startswith("q_")
        assert len(store) == 1
        os.unlink(path)

    def test_initial_status(self):
        store, path = self._make_store()
        qid = store.add("Glucose", {})
        record = store.get_record(qid)
        assert record["status"] == "pending_review"
        os.unlink(path)

    def test_valid_transition(self):
        store, path = self._make_store()
        qid = store.add("Glucose", {})
        store.update_status(qid, "in_review")
        assert store.get_record(qid)["status"] == "in_review"
        store.update_status(qid, "resolved", resolved_loinc="2345-7")
        assert store.get_record(qid)["status"] == "resolved"
        os.unlink(path)

    def test_invalid_transition_raises(self):
        store, path = self._make_store()
        qid = store.add("Glucose", {})
        with pytest.raises(ValueError, match="Cannot transition"):
            store.update_status(qid, "sent")
        os.unlink(path)

    def test_get_pending(self):
        store, path = self._make_store()
        store.add("Test1", {})
        store.add("Test2", {})
        qid3 = store.add("Test3", {})
        store.update_status(qid3, "in_review")
        pending = store.get_pending()
        assert len(pending) == 2
        os.unlink(path)

    def test_persistence(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        with open(path, "w") as f:
            json.dump({}, f)

        store1 = QuarantineStore(path=path)
        store1.add("Glucose", {"value": "95"})

        store2 = QuarantineStore(path=path)
        assert len(store2) == 1
        os.unlink(path)

    def test_nonexistent_record(self):
        store, path = self._make_store()
        assert store.get_record("nonexistent") is None
        with pytest.raises(ValueError):
            store.update_status("nonexistent", "resolved")
        os.unlink(path)
