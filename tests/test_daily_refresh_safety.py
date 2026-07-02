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
        payload = json.loads(data_path.read_text(encoding="utf-8"))
        records = payload.get("records", [])
        assert isinstance(records, list) and records, f"{county}: no records exported"


def test_enabled_counties_have_first_seen_dates():
    registry = json.loads((REPO / "dashboard" / "counties.json").read_text(encoding="utf-8"))
    for county, cfg in registry.items():
        if not cfg.get("enabled"):
            continue
        payload = json.loads((REPO / cfg["data_url"]).read_text(encoding="utf-8"))
        records = payload.get("records", [])
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
