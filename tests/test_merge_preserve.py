"""Safety tests for the merge-preserve layer (scraper/merge_preserve.py).

These encode the non-negotiable rules from the 2026-07-01 audit:
 1. A source failure must never overwrite good dashboard data with less
    or empty data.
 2. Historical records are preserved (carried forward, not deleted).
 3. first_seen_date is never rewritten - Today's Leads stays honest.
 4. No fake data: every merged record existed in the fresh pull or the
    previously committed file.
"""
import importlib.util
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "merge_preserve", REPO / "scraper" / "merge_preserve.py")
mp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mp)

TODAY = datetime.now().date().isoformat()
YESTERDAY = (datetime.now().date() - timedelta(days=1)).isoformat()


def rec(doc_num="", parcel="", owner="", addr="", city="AKRON", state="OH",
        first_seen=YESTERDAY, **extra):
    r = {"doc_num": doc_num, "parcel_id": parcel, "owner": owner,
         "prop_address": addr, "prop_city": city, "prop_state": state,
         "first_seen_date": first_seen, "score": 50, "flags": []}
    r.update(extra)
    return r


def payload(records, fetched_at=None):
    return {"fetched_at": fetched_at or f"{YESTERDAY}T12:00:00+00:00",
            "total": len(records), "records": records}


def test_shrunk_pull_carries_forward_missing_records():
    prev = payload([rec(doc_num="A1"), rec(doc_num="A2"), rec(doc_num="A3")])
    new = payload([rec(doc_num="A1", first_seen=YESTERDAY)])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    assert merged["total"] == 3
    doc_nums = {r["doc_num"] for r in merged["records"]}
    assert doc_nums == {"A1", "A2", "A3"}
    carried = [r for r in merged["records"] if r.get("carried_forward")]
    assert {r["doc_num"] for r in carried} == {"A2", "A3"}


def test_empty_pull_never_wipes_previous_data():
    prev = payload([rec(doc_num="A1"), rec(doc_num="A2")])
    new = payload([])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    assert len(merged["records"]) == 2
    assert merged is prev  # previous payload returned untouched


def test_record_count_never_shrinks():
    prev = payload([rec(doc_num=f"D{i}") for i in range(10)])
    new = payload([rec(doc_num="D0"), rec(doc_num="NEW1", first_seen="")])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    assert merged["total"] >= len(prev["records"])


def test_first_seen_date_preserved_on_carried_records():
    prev = payload([rec(doc_num="A1", first_seen="2026-05-01")])
    new = payload([rec(doc_num="B9", first_seen=TODAY)])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    a1 = next(r for r in merged["records"] if r["doc_num"] == "A1")
    assert a1["first_seen_date"] == "2026-05-01"
    assert a1["carried_forward"] is True


def test_no_fabricated_records():
    prev = payload([rec(doc_num="A1"), rec(doc_num="A2")])
    new = payload([rec(doc_num="A2"), rec(doc_num="A3")])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    allowed = {"A1", "A2", "A3"}
    assert {r["doc_num"] for r in merged["records"]} <= allowed
    assert merged["total"] == 3


def test_fresh_records_get_last_seen_today():
    prev = payload([rec(doc_num="A1")])
    new = payload([rec(doc_num="A1")])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    a1 = next(r for r in merged["records"] if r["doc_num"] == "A1")
    assert a1["last_seen_date"] == TODAY
    assert not a1.get("carried_forward")


def test_matching_by_owner_address_when_no_ids():
    prev = payload([rec(owner="JOHN DOE", addr="123 MAIN ST")])
    new = payload([rec(owner="John Doe", addr="123 MAIN ST")])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    assert merged["total"] == 1  # matched, not duplicated


def test_carried_records_keep_score_and_flags_untouched():
    prev = payload([rec(doc_num="A1", score=87, flags=["Tax delinquent", "Vacant"])])
    new = payload([rec(doc_num="ZZ", first_seen=TODAY)])
    merged = mp.merge_payload_preserving_previous(new, prev, today=TODAY)
    a1 = next(r for r in merged["records"] if r["doc_num"] == "A1")
    assert a1["score"] == 87
    assert a1["flags"] == ["Tax delinquent", "Vacant"]


def test_missing_previous_file_is_safe(tmp_path):
    new = payload([rec(doc_num="A1")])
    merged = mp.merge_with_previous_file(new, tmp_path / "does_not_exist.json",
                                          today=TODAY)
    assert merged["total"] == 1


def test_corrupt_previous_file_is_safe(tmp_path):
    bad = tmp_path / "records.json"
    bad.write_text("{not valid json", encoding="utf-8")
    new = payload([rec(doc_num="A1")])
    merged = mp.merge_with_previous_file(new, bad, today=TODAY)
    assert merged["total"] == 1


def test_sharded_previous_file_still_carries_forward(tmp_path):
    """2026-08-20 fix: once a previous records.json is big enough to be
    sharded (scraper/shard_utils.py), it has no inline "records" list at
    all - merge_with_previous_file must expand it back to a full payload
    first, or every carried-forward record would silently vanish the
    moment sharding kicks in."""
    su_spec = importlib.util.spec_from_file_location(
        "shard_utils", REPO / "scraper" / "shard_utils.py")
    su = importlib.util.module_from_spec(su_spec)
    su_spec.loader.exec_module(su)

    prev_path = tmp_path / "records.json"
    prev_full = payload([rec(doc_num="A1"), rec(doc_num="A2"), rec(doc_num="A3")])
    manifest = su.shard_payload(prev_full, prev_path, max_per_shard=1)
    assert manifest["sharded"] is True
    assert manifest["records"] == []  # sanity: this is the failure mode being tested
    prev_path.write_text(json.dumps(manifest), encoding="utf-8")

    new = payload([rec(doc_num="A1")])
    merged = mp.merge_with_previous_file(new, prev_path, today=TODAY)

    assert merged["total"] == 3
    doc_nums = {r["doc_num"] for r in merged["records"]}
    assert doc_nums == {"A1", "A2", "A3"}
    carried = {r["doc_num"] for r in merged["records"] if r.get("carried_forward")}
    assert carried == {"A2", "A3"}


def test_plain_previous_file_unaffected_by_shard_awareness(tmp_path):
    """Backward compatibility: a records.json written before sharding
    existed has no "sharded" key at all and must merge exactly as it did
    before this fix."""
    prev_path = tmp_path / "records.json"
    prev_path.write_text(json.dumps(payload([rec(doc_num="A1")])), encoding="utf-8")
    new = payload([rec(doc_num="B1")])
    merged = mp.merge_with_previous_file(new, prev_path, today=TODAY)
    assert {r["doc_num"] for r in merged["records"]} == {"A1", "B1"}
