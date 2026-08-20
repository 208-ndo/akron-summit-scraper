"""Tests for scraper/shard_utils.py - the fix for Summit's records.json
outgrowing GitHub's 100MB per-file push limit (root-caused 2026-08-20;
see the module docstring in scraper/shard_utils.py for the full story).

These encode the guarantees the rest of the pipeline depends on:
 1. Sharding never loses or reorders records.
 2. unshard_payload() is a no-op on a plain, unsharded payload, so every
    existing records.json ever committed stays readable.
 3. load_payload()/count_records() give identical answers whether or not
    a file happens to be sharded right now - callers should never need
    to know which form they're looking at.
"""
import importlib.util
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "shard_utils", REPO / "scraper" / "shard_utils.py")
su = importlib.util.module_from_spec(spec)
spec.loader.exec_module(su)


def make_payload(n):
    return {
        "fetched_at": "2026-08-20T00:00:00+00:00",
        "source": "test",
        "total": n,
        "records": [{"doc_num": f"D{i}", "score": i} for i in range(n)],
    }


def test_small_payload_is_not_sharded(tmp_path):
    payload = make_payload(3)
    manifest = su.shard_payload(payload, tmp_path / "records.json", max_per_shard=6000)
    # Still gets a manifest shape (single shard) - that's fine; the real
    # gate is SHARD_THRESHOLD_BYTES in tools/run_daily_refresh_all.py,
    # which decides whether to call this at all.
    assert manifest["record_count"] == 3
    assert manifest["shard_count"] == 1


def test_shard_then_unshard_round_trips_exactly(tmp_path):
    payload = make_payload(25000)
    out_path = tmp_path / "records.json"
    manifest = su.shard_payload(payload, out_path, max_per_shard=6000)
    out_path.write_text(json.dumps(manifest), encoding="utf-8")

    assert manifest["sharded"] is True
    assert manifest["records"] == []
    assert manifest["shard_count"] == 5  # ceil(25000/6000)
    assert len(manifest["shard_files"]) == 5

    for rel in manifest["shard_files"]:
        assert (tmp_path / rel).is_file()

    restored = su.unshard_payload(json.loads(out_path.read_text()), out_path.parent)
    assert [r["doc_num"] for r in restored["records"]] == [f"D{i}" for i in range(25000)]
    assert restored["fetched_at"] == payload["fetched_at"]


def test_unshard_is_noop_on_plain_payload(tmp_path):
    payload = make_payload(10)
    same = su.unshard_payload(payload, tmp_path)
    assert same is payload


def test_load_payload_transparent_for_both_forms(tmp_path):
    plain_path = tmp_path / "plain.json"
    plain_path.write_text(json.dumps(make_payload(5)), encoding="utf-8")
    plain = su.load_payload(plain_path)
    assert len(plain["records"]) == 5

    sharded_path = tmp_path / "records.json"
    manifest = su.shard_payload(make_payload(12000), sharded_path, max_per_shard=6000)
    sharded_path.write_text(json.dumps(manifest), encoding="utf-8")
    expanded = su.load_payload(sharded_path)
    assert len(expanded["records"]) == 12000


def test_count_records_matches_for_both_forms(tmp_path):
    plain_path = tmp_path / "plain.json"
    plain_path.write_text(json.dumps(make_payload(7)), encoding="utf-8")
    assert su.count_records(plain_path) == 7

    sharded_path = tmp_path / "records.json"
    manifest = su.shard_payload(make_payload(9000), sharded_path, max_per_shard=6000)
    sharded_path.write_text(json.dumps(manifest), encoding="utf-8")
    assert su.count_records(sharded_path) == 9000


def test_missing_shard_file_degrades_instead_of_crashing(tmp_path):
    out_path = tmp_path / "records.json"
    manifest = su.shard_payload(make_payload(12000), out_path, max_per_shard=6000)
    # Delete one shard to simulate a partial/corrupt checkout.
    (tmp_path / manifest["shard_files"][0]).unlink()
    restored = su.unshard_payload(manifest, tmp_path)
    assert len(restored["records"]) == 6000  # only the surviving shard


def test_reshardable_after_shrinking_shard_count(tmp_path):
    """A previous run left 5 shard files; this run only needs 2. The old
    orphans must not linger and get carried into a future unshard()."""
    out_path = tmp_path / "records.json"
    big_manifest = su.shard_payload(make_payload(25000), out_path, max_per_shard=6000)
    assert big_manifest["shard_count"] == 5

    small_manifest = su.shard_payload(make_payload(3000), out_path, max_per_shard=6000)
    assert small_manifest["shard_count"] == 1
    remaining = sorted(p.name for p in (tmp_path / "records_shards").glob("*.json"))
    assert remaining == ["records_0001.json"]
