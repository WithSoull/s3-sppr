import hashlib
import json
import math

import numpy as np

from sppr_ml import data
from sppr_ml.data import FEATURES


def write_dataset(path, rows):
    path.mkdir()
    body = "".join(json.dumps(r) + "\n" for r in rows)
    (path / "slices.jsonl").write_text(body)
    manifest = {
        "feature_rules_version": "features-v1", "horizon_seconds": 3600,
        "train": {"start": "2026-10-01T00:00:00Z", "end": "2026-10-03T00:00:00Z"},
        "validation": {"start": "2026-10-03T00:00:00Z", "end": "2026-10-04T00:00:00Z"},
        "test": {"start": "2026-10-04T00:00:00Z", "end": "2026-10-08T00:00:00Z"},
        "sha256": {"slices.jsonl": hashlib.sha256(body.encode()).hexdigest()},
    }
    (path / "manifest.json").write_text(json.dumps(manifest))


def row(at, split, reads, eligible=True, last=None):
    feats = {n: 1.0 for n in FEATURES}
    feats["last_read_seconds"] = last
    return {
        "snapshot": {"version_id": "v1", "at": at, "size_bytes": 10, "rules_version": "features-v1", "features": feats},
        "forecast": {"expected_reads": 0.5},
        "split": split, "label": {"reads": reads}, "eligible": eligible,
    }


def test_load_keeps_eligible_rows_thins_train_and_hides_test(tmp_path):
    rows = [
        row("2026-10-01T00:00:00Z", "train", 1, last=60.0),
        row("2026-10-01T00:05:00Z", "train", 2),
        row("2026-10-01T00:15:00Z", "train", 3),
        row("2026-10-01T00:20:00Z", "train", 4, eligible=False),
        row("2026-10-03T00:05:00Z", "validation", 5),
        row("2026-10-04T00:00:00Z", "test", 6),
    ]
    write_dataset(tmp_path / "d", rows)
    s = data.load(tmp_path / "d", train_every=900)
    assert s.reads.tolist() == [1, 3, 5]
    assert s.feature("last_read_seconds")[0] == 60.0 and math.isnan(s.feature("last_read_seconds")[1])
    assert s.ewma.tolist() == [0.5, 0.5, 0.5]
    # The second load reads the cache and the test row appears only on request.
    assert sorted(data.load(tmp_path / "d", include_test=True).reads.tolist()) == [1, 2, 3, 5, 6]
    assert list((tmp_path / "d").glob("slices.*.npz"))
