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
    assert list((tmp_path / "d").glob("slices.*.v2"))


def write_compact(path, rows):
    import csv
    import gzip
    import io

    path.mkdir()
    header = ["version_id", "at", "size_bytes", "rules_version", *FEATURES, "label_reads", "eligible", "split", "ewma_forecast"]
    hashes = {}
    for split in data.SPLITS:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(header)
        for r in rows:
            if r["split"] != split:
                continue
            f = r["snapshot"]["features"]
            w.writerow(["v1", r["snapshot"]["at"], 10, "features-v1",
                        *["" if f[n] is None else f[n] for n in FEATURES],
                        r["label"]["reads"], str(r["eligible"]).lower(), split, 0.5])
        blob = gzip.compress(buf.getvalue().encode())
        (path / f"{split}.csv.gz").write_bytes(blob)
        hashes[f"{split}.csv.gz"] = hashlib.sha256(blob).hexdigest()
    manifest = json.loads((path.parent / "j" / "manifest.json").read_text())
    manifest.update(format="csv.gz", sha256=hashes)
    (path / "manifest.json").write_text(json.dumps(manifest))


def test_compact_format_matches_jsonl_and_never_parses_test(tmp_path):
    rows = [
        row("2026-10-01T00:00:00Z", "train", 1, last=60.0),
        row("2026-10-01T00:05:00Z", "train", 2),
        row("2026-10-01T00:20:00Z", "train", 4, eligible=False),
        row("2026-10-03T00:05:00Z", "validation", 5),
        row("2026-10-04T00:00:00Z", "test", 6),
    ]
    write_dataset(tmp_path / "j", rows)
    write_compact(tmp_path / "c", rows)
    j, c = data.load(tmp_path / "j"), data.load(tmp_path / "c")
    np.testing.assert_array_equal(j.x, c.x)
    for name in ("reads", "at", "split", "version", "ewma", "size"):
        np.testing.assert_array_equal(getattr(j, name), getattr(c, name))
    assert not list((tmp_path / "c").glob("test.*.v2"))
    assert j.data_sha256 == j.manifest["sha256"]["slices.jsonl"]
    assert c.data_sha256 == hashlib.sha256((tmp_path / "c" / "manifest.json").read_bytes()).hexdigest()
    assert data.load(tmp_path / "c", include_test=True).reads.tolist() == [1, 2, 5, 6]


def test_cache_spans_chunks_and_merges_vocabularies(tmp_path):
    rows = [([float(i)] * len(FEATURES), i, 100 + i, i % 2, f"v{i % 3}", 0.5 * i, 10 * i) for i in range(7)]
    data._write_cache(iter(rows), tmp_path / "a", chunk=3)
    data._write_cache(iter([([1.0] * len(FEATURES), 9, 200, 1, "v9", 0.0, 1), rows[1]]), tmp_path / "b", chunk=3)
    cols = data._read([tmp_path / "a", tmp_path / "b"])
    assert cols["x"].shape == (9, len(FEATURES)) and cols["x"][6, 0] == 6.0
    assert [str(cols["versions"][c]) for c in cols["version"]] == [f"v{i % 3}" for i in range(7)] + ["v9", "v1"]
    assert cols["at"].dtype == np.int64 and cols["at"].tolist() == list(range(100, 107)) + [200, 101]
    data._write_cache(iter([]), tmp_path / "e")
    assert data._read([tmp_path / "e"])["x"].shape == (0, len(FEATURES))


def test_split_is_a_view_when_contiguous(tmp_path):
    rows = [row("2026-10-01T00:00:00Z", "train", 1), row("2026-10-01T00:15:00Z", "train", 2),
            row("2026-10-03T00:05:00Z", "validation", 5)]
    write_dataset(tmp_path / "d", rows)
    s = data.load(tmp_path / "d")
    assert np.shares_memory(s.of("validation").x, s.x)
    assert s.of("train").reads.tolist() == [1, 2] and s.of("validation").reads.tolist() == [5]
    interleaved = s.take(np.array([2, 0, 1]))
    assert interleaved.of("train").reads.tolist() == [1, 2]
