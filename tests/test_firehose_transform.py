import base64
import json

import firehose_transform as ft
from conftest import b64json, cdc


def _run(*payloads, raw=None):
    records = [{"recordId": str(i), "data": b64json(p)} for i, p in enumerate(payloads)]
    if raw is not None:
        records.append({"recordId": "raw", "data": raw})
    return ft.lambda_handler({"records": records}, None)["records"]


def _decode(rec):
    return json.loads(base64.b64decode(rec["data"]))


def test_create_is_reshaped_to_iceberg_columns():
    [rec] = _run(cdc(row={"id": 7, "total": 1.25}))
    assert rec["recordId"] == "0" and rec["result"] == "Ok"
    out = _decode(rec)
    assert out["source_table"] == "orders"
    assert out["operation"] == "c"
    assert out["record_id"] == "7"
    assert json.loads(out["event_data"]) == {"id": 7, "total": 1.25}
    assert out["commit_timestamp"] == 1_780_615_647_656_000  # ms -> us
    assert isinstance(out["ingested_at"], int)
    assert base64.b64decode(rec["data"]).endswith(b"\n")


def test_delete_uses_before_image():
    [rec] = _run(cdc(op="d", before={"id": "abc"}))
    out = _decode(rec)
    assert out["operation"] == "d" and out["record_id"] == "abc"


def test_poison_records_are_dropped_not_errored():
    recs = _run(
        {"op": "u", "after": {"id": 1}, "ts_ms": 1},  # unknown op
        {"op": "d", "before": {}, "ts_ms": 1},  # empty delete payload
        cdc(row={"total": 1}),  # missing id
        {"op": "c", "after": {"id": 1}},  # missing ts_ms
        raw="not base64 json!!",
    )
    assert [r["result"] for r in recs] == ["Dropped"] * 5
    assert all("data" not in r for r in recs)


def test_missing_source_defaults_table_and_batch_shares_ingested_at():
    p1 = cdc()
    p1.pop("source")
    recs = _run(p1, cdc(row={"id": 2}))
    a, b = (_decode(r) for r in recs)
    assert a["source_table"] == "unknown"
    assert a["ingested_at"] == b["ingested_at"]


def test_row_for_op_matches_hot_path():
    import cdc_processor

    for p in [cdc(), cdc(op="d"), {"op": "x"}, {"op": "c", "after": None}]:
        assert ft._row_for_op(p) == cdc_processor._row_for_op(p)
