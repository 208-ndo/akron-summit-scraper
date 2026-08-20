"""Safety tests for the daily refresh orchestration and dashboard exports.

Covers audit requirements:
 - Today's Leads only counts records first seen today.
 - A failure in one county does not stop the other counties.
 - The status updater is honest (last_checked always moves, data_updated
   only when explicitly confirmed).
 - Every enabled county in the registry exports dashboard JSON.
 - Stale-source information is present in the status file schema so the
   dashboard warning can render.
"""
import importlib.util
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

import run_daily_refresh_all as orch  # noqa: E402

_su_spec = importlib.util.spec_from_file_location("shard_utils", REPO / "scraper" / "shard_utils.py")
_su = importlib.util.module_from_spec(_su_spec)
_su_spec.loader.exec_module(_su)


def _load_records(data_path):
    """Shard-aware read for tests below: dashboard/records.json may be a
    small shard manifest rather than a plain payload once it's grown
    past SHARD_THRESHOLD_BYTES (2026-08-20 fix) - see
    scraper/shard_utils.py."""
    payload = _su.load_payload(data_path)
    return payload.get("records", []) if payload else []

TODAY = datetime.now().date().isoformat()
YESTERDAY = (datetime.now().date() - timedelta(days=1)).isoformat()


# ---------------------------------------------------------------- Today's Leads
def test_count_new_today_only_counts_first_seen_today(tmp_path):
    f = tmp_path / "records.json"
    f.write_text(json.dumps({"records": [
        {"first_seen_date": TODAY},
        {"first_seen_date": TODAY},
        {"first_seen_date": YESTERDAY},                 # old lead
        {"first_seen_date": YESTERDAY, "last_updated_date": TODAY},  # touched today, NOT new
        {"first_seen_date": ""},
    ]}), encoding="utf-8")
    assert orch.count_new_today(f) == 2


def test_count_new_today_zero_on_missing_file(tmp_path):
    assert orch.count_new_today(tmp_path / "nope.json") == 0


def test_count_new_today_reads_through_a_sharded_file(tmp_path):
    """2026-08-20 fix: once records.json is sharded, a plain json.loads
    here would find an empty inline "records" list and always report 0
    new leads - count_new_today must expand it first."""
    f = tmp_path / "records.json"
    manifest = _su.shard_payload({"records": [
        {"first_seen_date": TODAY}, {"first_seen_date": TODAY},
        {"first_seen_date": YESTERDAY},
    ]}, f, max_per_shard=1)
    f.write_text(json.dumps(manifest), encoding="utf-8")
    assert orch.count_new_today(f) == 2


# ---------------------------------------------------- oversized-file sharding
def test_small_file_is_left_alone(tmp_path):
    f = tmp_path / "records.json"
    f.write_text(json.dumps({"records": [{"doc_num": "A1"}]}), encoding="utf-8")
    orch.maybe_shard_large_file(f, log=lambda *_: None)
    payload = json.loads(f.read_text())
    assert not payload.get("sharded")
    assert payload["records"] == [{"doc_num": "A1"}]


def test_oversized_file_gets_sharded_and_record_count_is_preserved(tmp_path, monkeypatch):
    monkeypatch.setattr(orch, "SHARD_THRESHOLD_BYTES", 100)  # force the threshold for the test
    f = tmp_path / "records.json"
    records = [{"doc_num": f"D{i}", "note": "x" * 50} for i in range(500)]
    f.write_text(json.dumps({"fetched_at": "2026-08-20T00:00:00+00:00", "records": records}), encoding="utf-8")
    before_size = f.stat().st_size
    assert before_size > 100

    orch.maybe_shard_large_file(f, log=lambda *_: None)

    after_size = f.stat().st_size
    payload = json.loads(f.read_text())
    assert payload["sharded"] is True
    assert after_size < before_size  # the committed file itself shrank
    assert orch.count_records(f) == 500  # but no records were lost


def test_shard_step_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(orch, "SHARD_THRESHOLD_BYTES", 100)
    f = tmp_path / "records.json"
    records = [{"doc_num": f"D{i}"} for i in range(500)]
    f.write_text(json.dumps({"records": records}), encoding="utf-8")
    orch.maybe_shard_large_file(f, log=lambda *_: None)
    orch.maybe_shard_large_file(f, log=lambda *_: None)  # already sharded - must not double-wrap
    assert orch.count_records(f) == 500


# ------------------------------------------------------- county isolation
def test_one_county_failure_does_not_stop_others(monkeypatch, tmp_path):
    calls = []

    def ok_county(name):
        def fn(commit, push):
            calls.append(name)
            return {"county": name, "status": "success_no_change"}
        return fn

    def boom(commit, push):
        calls.append("summit")
        raise RuntimeError("simulated summit crash")

    monkeypatch.setattr(orch, "refresh_summit", boom)
    monkeypatch.setattr(orch, "refresh_cuyahoga", ok_county("cuyahoga"))
    monkeypatch.setattr(orch, "refresh_montgomery", ok_county("montgomery"))
    monkeypatch.setattr(orch, "update_status", lambda *a, **k: None)
    monkeypatch.setattr(orch, "LOG_DIR", tmp_path)

    rc = orch.main([])  # all three counties, dry run (no --commit/--push)
    assert rc == 0
    assert calls == ["summit", "cuyahoga", "montgomery"]


# --------------------------------------------------- loud failure on rejected push
def test_main_fails_loudly_when_a_push_is_rejected(monkeypatch, tmp_path):
    """2026-08-20 fix: this is the actual bug that froze Summit's data for
    23 days. A rejected push (e.g. a file over GitHub's 100MB limit) used
    to be logged by git_push_with_retry() and then silently ignored - the
    job exited 0 and Actions stayed green. It must now fail the run."""
    def fake_summit(commit, push):
        return {"county": "summit", "status": "success_updated", "push_failed": True}

    def ok_county(name):
        def fn(commit, push):
            return {"county": name, "status": "success_no_change"}
        return fn

    monkeypatch.setattr(orch, "refresh_summit", fake_summit)
    monkeypatch.setattr(orch, "refresh_cuyahoga", ok_county("cuyahoga"))
    monkeypatch.setattr(orch, "refresh_montgomery", ok_county("montgomery"))
    monkeypatch.setattr(orch, "update_status", lambda *a, **k: None)
    monkeypatch.setattr(orch, "LOG_DIR", tmp_path)

    rc = orch.main(["--commit", "--push"])
    assert rc == 1


def test_main_succeeds_when_pushes_all_land(monkeypatch, tmp_path):
    def ok_county(name):
        def fn(commit, push):
            return {"county": name, "status": "success_no_change", "push_failed": False}
        return fn

    monkeypatch.setattr(orch, "refresh_summit", ok_county("summit"))
    monkeypatch.setattr(orch, "refresh_cuyahoga", ok_county("cuyahoga"))
    monkeypatch.setattr(orch, "refresh_montgomery", ok_county("montgomery"))
    monkeypatch.setattr(orch, "update_status", lambda *a, **k: None)
    monkeypatch.setattr(orch, "LOG_DIR", tmp_path)

    rc = orch.main(["--commit", "--push"])
    assert rc == 0


# ------------------------------------------------------- status file honesty
def _load_status_module():
    spec = importlib.util.spec_from_file_location(
        "update_refresh_status", REPO / "tools" / "update_refresh_status.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_status_updater_moves_last_checked_but_not_data_updated(tmp_path, monkeypatch):
    mod = _load_status_module()
    status_path = tmp_path / "refresh_status.json"
    status_path.write_text(json.dumps({"summit": {
        "data_updated_at": "2026-06-23T00:00:00+00:00",
        "source_dataset_date": "2026-06-20",
        "last_checked_at": "2026-06-23T00:00:00+00:00",
    }}), encoding="utf-8")
    monkeypatch.setattr(mod, "STATUS_PATH", status_path)

    mod.main(["--county", "summit", "--status", "success_no_change",
              "--message", "no new data"])
    data = json.loads(status_path.read_text(encoding="utf-8"))
    entry = data["summit"]
    assert entry["data_updated_at"] == "2026-06-23T00:00:00+00:00"  # untouched
    assert entry["source_dataset_date"] == "2026-06-20"             # untouched
    assert entry["last_checked_at"] != "2026-06-23T00:00:00+00:00"  # moved
    assert entry["status"] == "success_no_change"


def test_status_updater_records_stale_source_for_warning(tmp_path, monkeypatch):
    mod = _load_status_module()
    status_path = tmp_path / "refresh_status.json"
    monkeypatch.setattr(mod, "STATUS_PATH", status_path)
    mod.main(["--county", "montgomery", "--status", "stale_source",
              "--message", "Source CSV still dated 2026-06-04",
              "--source-dataset-date", "2026-06-04"])
    entry = json.loads(status_path.read_text(encoding="utf-8"))["montgomery"]
    # These two fields are what the dashboard stale warning renders from.
    assert entry["status"] == "stale_source"
    assert entry["source_dataset_date"] == "2026-06-04"


# ------------------------------------------------------- county exports
def test_every_enabled_county_exports_dashboard_json():
    registry = json.loads((REPO / "dashboard" / "counties.json").read_text(encoding="utf-8"))
    enabled = {k: v for k, v in registry.items() if v.get("enabled")}
    assert enabled, "county registry has no enabled counties"
    for county, cfg in enabled.items():
        data_path = REPO / cfg["data_url"]
        assert data_path.is_file(), f"{county}: missing {cfg['data_url']}"
        records = _load_records(data_path)
        assert isinstance(records, list) and records, f"{county}: no records exported"


def test_enabled_counties_have_first_seen_dates():
    registry = json.loads((REPO / "dashboard" / "counties.json").read_text(encoding="utf-8"))
    for county, cfg in registry.items():
        if not cfg.get("enabled"):
            continue
        records = _load_records(REPO / cfg["data_url"])
        with_fsd = sum(1 for r in records if str(r.get("first_seen_date") or "").strip())
        assert with_fsd / max(len(records), 1) > 0.5, (
            f"{county}: most records lack first_seen_date - Today's Leads would be dishonest")


def test_status_file_covers_all_enabled_counties():
    registry = json.loads((REPO / "dashboard" / "counties.json").read_text(encoding="utf-8"))
    status = json.loads((REPO / "dashboard" / "refresh_status.json").read_text(encoding="utf-8"))
    for county, cfg in registry.items():
        if cfg.get("enabled"):
            assert county in status, f"{county} enabled but has no refresh status entry"
            assert status[county].get("last_checked_at"), f"{county}: no last_checked_at"
