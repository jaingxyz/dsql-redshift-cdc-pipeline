import pytest

import cdc_processor as cp
from conftest import b64json, cdc


class FakeRedshift:
    def __init__(self, statuses=None):
        self.statements = []
        self.statuses = list(statuses or [])

    def execute_statement(self, **kw):
        self.statements.append(kw)
        return {"Id": f"stmt-{len(self.statements)}"}

    def describe_statement(self, Id):
        status = self.statuses.pop(0) if self.statuses else "FINISHED"
        return {"Status": status, "Error": "boom"}


@pytest.fixture
def fake(monkeypatch):
    f = FakeRedshift()
    monkeypatch.setattr(cp, "redshift", f)
    monkeypatch.setattr(cp.time, "sleep", lambda _s: None)
    return f


def _event(*payloads, raw=()):
    recs = [{"kinesis": {"data": b64json(p)}} for p in payloads]
    recs += [{"kinesis": {"data": r}} for r in raw]
    return {"Records": recs}


def test_insert_is_fully_parameterized():
    rows = [
        {
            "table": "orders",
            "op": "c",
            "record_id": 5,
            "row": {"id": 5},
            "commit_ts_ms": 10,
        },
        {
            "table": "t'; DROP TABLE x;--",
            "op": "d",
            "record_id": 6,
            "row": {},
            "commit_ts_ms": 11,
        },
    ]
    sql, params = cp._build_parameterized_insert(rows)
    assert "DROP" not in sql  # values never interpolated into SQL text
    assert sql.count("JSON_PARSE(") == 2
    assert len(params) == 2 * cp.PARAMS_PER_ROW
    assert {"name": "id0", "value": "5"} in params
    assert {"name": "t1", "value": "t'; DROP TABLE x;--"} in params


def test_chunks_stay_under_data_api_parameter_cap(fake):
    payloads = [cdc(row={"id": i}) for i in range(100)]
    result = cp.lambda_handler(_event(*payloads), None)
    assert result["processed"] == 100 and result["chunks"] == 3
    for s in fake.statements:
        assert len(s["Parameters"]) <= cp.MAX_PARAMS_PER_STATEMENT
        assert s["WorkgroupName"] == "test-wg" and s["Database"] == "dev"


def test_poison_records_skipped_without_sql(fake):
    result = cp.lambda_handler(
        _event(
            {"op": "u"},
            cdc(row={"total": 1}),
            {"op": "c", "after": {"id": 1}},
            raw=["%%%"],
        ),
        None,
    )
    assert result == {"processed": 0, "skipped": 4}
    assert fake.statements == []


@pytest.mark.parametrize("status", ["FAILED", "ABORTED"])
def test_failed_statement_raises_so_kinesis_retries(fake, status):
    fake.statuses = ["SUBMITTED", status]
    with pytest.raises(RuntimeError, match=status):
        cp.lambda_handler(_event(cdc()), None)


def test_statement_timeout_raises(fake, monkeypatch):
    fake.statuses = ["STARTED"] * 10
    ticks = iter(range(0, 1000, 10))
    monkeypatch.setattr(cp.time, "monotonic", lambda: next(ticks))
    with pytest.raises(RuntimeError, match="did not finish"):
        cp._await_statement("stmt-x")
