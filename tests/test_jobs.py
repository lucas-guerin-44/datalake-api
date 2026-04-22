"""
Tests for the in-memory background job registry.
"""
import os


import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services import jobs


@pytest.fixture(autouse=True)
def clean_registry(monkeypatch):
    monkeypatch.setattr(jobs, "_JOBS", {})
    yield


class TestJobsRegistry:

    def test_create_returns_running_job(self):
        job = jobs.create_job("test", meta={"foo": "bar"})
        assert job.status == "running"
        assert job.kind == "test"
        assert job.meta == {"foo": "bar"}
        assert job.finished_at is None
        assert jobs.get_job(job.id) is job

    def test_finish_sets_ok_and_result(self):
        job = jobs.create_job("test")
        jobs.finish_job(job.id, result={"count": 42})
        fetched = jobs.get_job(job.id)
        assert fetched.status == "ok"
        assert fetched.result == {"count": 42}
        assert fetched.finished_at is not None

    def test_finish_with_error_sets_error_status(self):
        job = jobs.create_job("test")
        jobs.finish_job(job.id, error="boom")
        fetched = jobs.get_job(job.id)
        assert fetched.status == "error"
        assert fetched.error == "boom"

    def test_missing_job_returns_none(self):
        assert jobs.get_job("no-such-id") is None

    def test_finish_unknown_job_is_noop(self):
        jobs.finish_job("no-such-id", result={"x": 1})  # must not raise

    def test_list_returns_newest_first(self):
        import time
        a = jobs.create_job("a")
        time.sleep(0.001)  # datetime.now() can collide at microsecond resolution
        b = jobs.create_job("b")
        time.sleep(0.001)
        c = jobs.create_job("c")
        listed = jobs.list_jobs()
        ids = [j["id"] for j in listed]
        assert ids[:3] == [c.id, b.id, a.id]

    def test_to_dict_serializes_timestamps(self):
        job = jobs.create_job("test")
        jobs.finish_job(job.id, result={"ok": True})
        d = jobs.get_job(job.id).to_dict()
        assert isinstance(d["started_at"], str)
        assert isinstance(d["finished_at"], str)

    def test_eviction_caps_registry_size(self, monkeypatch):
        monkeypatch.setattr(jobs, "MAX_JOBS", 3)

        j1 = jobs.create_job("a"); jobs.finish_job(j1.id, result={})
        j2 = jobs.create_job("b"); jobs.finish_job(j2.id, result={})
        j3 = jobs.create_job("c"); jobs.finish_job(j3.id, result={})
        j4 = jobs.create_job("d")  # running, must stay

        # Creating the 5th job over the cap evicts the oldest *finished* one (j1).
        jobs.create_job("e")
        assert jobs.get_job(j1.id) is None
        assert jobs.get_job(j4.id) is not None  # running, never evicted
