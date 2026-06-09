"""
Firehose transform Lambda for the Iceberg cold path.

Why this exists
---------------
Amazon Data Firehose's direct Kinesis -> Iceberg routing maps the
*top-level JSON keys* of each source record to destination columns by
name. The DSQL CDC records on our Kinesis stream look like:

    {"op": "c", "after": {...}, "before": null,
     "source": {"table": "orders", ...},
     "ts_ms": 1780615647656, "ts_ns": ...}

None of those keys match the `cdc_events_archive` Iceberg columns
(source_table, operation, record_id, event_data, commit_timestamp,
ingested_at). Without this transform, Firehose rejects 100% of records
with `Iceberg.MissingColumnWithinRecord` and dumps them in the error
bucket.

This function reshapes each DSQL CDC record into the Iceberg column
shape. The op/row extraction mirrors `cdc_processor._row_for_op` so the
cold path and the hot path agree on what each event means.

Output contract (Firehose data transformation):
  * One output record per input record, echoing `recordId`.
  * result="Ok"     -> reshaped JSON delivered to Iceberg.
  * result="Dropped"-> poison/unprocessable record; Firehose neither
                       delivers it nor sends it to the S3 error bucket
                       nor retries. Matches the hot path's `skipped`
                       semantics for malformed payloads. We Drop on:
                       undecodable JSON, unknown op type, op="d" with
                       empty/missing `before` payload (legitimate in
                       DSQL CDC if a delete fires for a row that was
                       already absent), and missing `id` / `ts_ms`.

Timestamps: Firehose requires Iceberg timestamp columns to be sent in
MICROSECONDS (see the "supported data types" docs). DSQL CDC carries
`ts_ms` (milliseconds); we multiply by 1000.

AppendOnly delivery: the Firehose stream is AppendOnly=true with a single
static destination table, so both "c" and "d" become appended rows
(operation column preserves which). No per-record routing metadata is
needed; current state is reconstructed downstream exactly as for the hot
path.
"""

import base64
import binascii
import json
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _row_for_op(payload: dict):
    """Extract (op, row_dict) from a CDC payload, or None if unprocessable.

    Mirrors cdc_processor._row_for_op: only "c" (create; also carries
    UPDATEs in preview) and "d" (delete) are processable.
    """
    op = payload.get("op")
    if op == "c":
        row = payload.get("after")
    elif op == "d":
        row = payload.get("before")
    else:
        logger.warning("Dropping unknown op: %s", op)
        return None
    if not row:
        logger.warning("Dropping op=%s with empty row payload", op)
        return None
    return (op, row)


def _reshape(payload: dict, ingested_us: int):
    """Map a DSQL CDC payload to the Iceberg column shape, or None to drop."""
    result = _row_for_op(payload)
    if not result:
        return None
    op, row = result

    record_id = row.get("id")
    ts_ms = payload.get("ts_ms")
    if record_id is None or ts_ms is None:
        # No PK or no commit timestamp: same poison guard as the hot path.
        # Logging field presence (not values) keeps PK/timestamps out of
        # CloudWatch - sample-grade hygiene. Bump to DEBUG and log values
        # if you need them for one-off debugging.
        logger.warning(
            "Dropping op=%s payload - id_present=%s ts_present=%s",
            op,
            record_id is not None,
            ts_ms is not None,
        )
        return None

    source = payload.get("source", {})
    return {
        "source_table": source.get("table", "unknown"),
        "operation": op,
        "record_id": str(record_id),
        # Full row state as a JSON string; Iceberg column is `string`,
        # mirroring the hot path's SUPER event_data.
        "event_data": json.dumps(row),
        # Firehose wants Iceberg timestamps in microseconds.
        "commit_timestamp": int(ts_ms) * 1000,
        "ingested_at": ingested_us,
    }


def lambda_handler(event, context):
    # Single wall-clock read per invocation: every record in this batch
    # shares one ingested_at, which is fine for an append-only archive.
    ingested_us = time.time_ns() // 1000

    out = []
    ok = dropped = 0
    for record in event.get("records", []):
        rid = record["recordId"]
        try:
            raw = base64.b64decode(record["data"])
            payload = json.loads(raw)
        except (KeyError, TypeError, ValueError, binascii.Error) as e:
            # Undecodable: drop rather than poison the error bucket.
            logger.warning("Dropping undecodable record %s: %s", rid, e)
            out.append({"recordId": rid, "result": "Dropped"})
            dropped += 1
            continue

        reshaped = _reshape(payload, ingested_us)
        if reshaped is None:
            out.append({"recordId": rid, "result": "Dropped"})
            dropped += 1
            continue

        data = base64.b64encode((json.dumps(reshaped) + "\n").encode("utf-8")).decode(
            "utf-8"
        )
        out.append({"recordId": rid, "result": "Ok", "data": data})
        ok += 1

    logger.info("Transformed batch: ok=%d dropped=%d", ok, dropped)
    return {"records": out}
