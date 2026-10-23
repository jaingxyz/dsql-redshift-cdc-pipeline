import base64
import json
import os
import sys
from pathlib import Path

# The Lambdas read config at import time; give them harmless test values.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("REDSHIFT_WORKGROUP", "test-wg")
os.environ.setdefault("REDSHIFT_DATABASE", "dev")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))


def b64json(obj) -> str:
    return base64.b64encode(json.dumps(obj).encode()).decode()


def cdc(op="c", row=None, table="orders", ts_ms=1_780_615_647_656, before=None):
    payload = {"op": op, "source": {"table": table}, "ts_ms": ts_ms}
    if op == "c":
        payload["after"] = row if row is not None else {"id": 1, "total": 9.5}
    else:
        payload["before"] = before if before is not None else {"id": 1}
    return payload
