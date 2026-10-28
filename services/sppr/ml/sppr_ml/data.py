"""Loading feature slices written by cmd/dataset.

Rows are kept as numpy columns. Both output formats are read: slices.jsonl and
the compact per-split <split>.csv.gz. Parsed rows are cached next to them, keyed
by the SHA-256 from the manifest, because parsing hundreds of megabytes dominates
a notebook run otherwise. Compact files are cached per split, so the test file is
never even parsed unless asked for.

The cache is one raw file per column, written chunk by chunk while parsing and
read straight into the final arrays: on the acceptance profile (4.7 M rows) any
intermediate copy of the feature matrix costs 0.6 GiB of the 2 GiB budget.
Version IDs are int32 codes into a vocabulary, since trace keys can be long.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np

RULES_VERSION = "features-v1"
HORIZON_SECONDS = 3600

# Column order is part of the model contract: Go reads feature_names from the manifest.
FEATURES = [
    "log_size",
    "age_seconds",
    "overwrites_24h",
    "last_read_seconds",
    "previous_read_seconds",
    "reads_5m",
    "reads_1h",
    "reads_6h",
    "reads_24h",
    "ewma_1h",
    "ewma_6h",
    "short_long_ratio",
    "reads_day_ago",
    "mean_read_fraction",
    "bucket_reads_1h",
    "bucket_active_fraction",
    "observed_seconds",
]
NULLABLE = {"last_read_seconds", "previous_read_seconds"}

SPLITS = ("train", "validation", "test")
_SPLIT_CODE = {name: i for i, name in enumerate(SPLITS)}
_CACHE_FORMAT = 2


class TestSplitLocked(RuntimeError):
    """Raised when stage 8 code asks for test rows without saying so explicitly."""


@dataclass
class Periods:
    train: tuple[int, int]
    validation: tuple[int, int]
    test: tuple[int, int]

    def bounds(self, split: str) -> tuple[int, int]:
        return getattr(self, split)


@dataclass
class Slices:
    """Eligible rows of one source. Times are Unix seconds."""

    source: str
    x: np.ndarray
    reads: np.ndarray
    at: np.ndarray
    split: np.ndarray
    version: np.ndarray
    ewma: np.ndarray
    size: np.ndarray
    periods: Periods
    manifest: dict = field(repr=False)
    # Model manifests cite it: the slices file, or for per-split files the
    # dataset manifest that lists their hashes ("Схема файлов экспорта v1").
    data_sha256: str = ""
    # version holds codes into this vocabulary.
    versions: np.ndarray = field(default_factory=lambda: np.array([], dtype=str), repr=False)

    def __len__(self) -> int:
        return len(self.reads)

    def take(self, mask: np.ndarray | slice) -> "Slices":
        return Slices(
            self.source, self.x[mask], self.reads[mask], self.at[mask], self.split[mask],
            self.version[mask], self.ewma[mask], self.size[mask], self.periods, self.manifest, self.data_sha256,
            self.versions,
        )

    def of(self, split: str) -> "Slices":
        rows = np.flatnonzero(self.split == _SPLIT_CODE[split])
        if len(rows) and rows[-1] - rows[0] + 1 == len(rows):
            # Splits are periods of time and rows are written in time order, so
            # a view avoids a second copy of the feature matrix.
            return self.take(slice(rows[0], rows[-1] + 1))
        return self.take(self.split == _SPLIT_CODE[split])

    def feature(self, name: str) -> np.ndarray:
        return self.x[:, FEATURES.index(name)]

    def version_id(self, i: int) -> str:
        return str(self.versions[self.version[i]])


def _ts(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def read_manifest(source: Path) -> dict:
    with open(source / "manifest.json", encoding="utf-8") as f:
        manifest = json.load(f)
    if manifest.get("feature_rules_version") != RULES_VERSION:
        raise ValueError(f"{source}: feature rules {manifest.get('feature_rules_version')}, want {RULES_VERSION}")
    if manifest.get("horizon_seconds") != HORIZON_SECONDS:
        raise ValueError(f"{source}: horizon {manifest.get('horizon_seconds')} s, want {HORIZON_SECONDS}")
    return manifest


def periods_of(manifest: dict) -> Periods:
    return Periods(*[(_ts(manifest[s]["start"]), _ts(manifest[s]["end"])) for s in SPLITS])


def _jsonl_rows(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if not row["eligible"]:
                continue
            snap = row["snapshot"]
            if snap["rules_version"] != RULES_VERSION:
                raise ValueError(f"unexpected rules version {snap['rules_version']}")
            feats = snap["features"]
            yield ([math.nan if feats[n] is None else float(feats[n]) for n in FEATURES],
                   row["label"]["reads"], _ts(snap["at"]), _SPLIT_CODE[row["split"]],
                   snap["version_id"], row["forecast"]["expected_reads"], snap["size_bytes"])


def _compact_rows(path: Path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        lines = csv.reader(f)
        col = {name: i for i, name in enumerate(next(lines))}
        feats = [col[n] for n in FEATURES]
        c_eligible, c_rules, c_reads = col["eligible"], col["rules_version"], col["label_reads"]
        c_at, c_split, c_version = col["at"], col["split"], col["version_id"]
        c_ewma, c_size = col["ewma_forecast"], col["size_bytes"]
        for r in lines:
            if r[c_eligible] != "true":
                continue
            if r[c_rules] != RULES_VERSION:
                raise ValueError(f"unexpected rules version {r[c_rules]}")
            yield ([float(r[i]) if r[i] else math.nan for i in feats], float(r[c_reads]), _ts(r[c_at]),
                   _SPLIT_CODE[r[c_split]], r[c_version], float(r[c_ewma]), int(r[c_size]))


_COLUMNS = (("x", np.float64), ("reads", np.float64), ("at", np.int64), ("split", np.int8),
            ("version", np.int32), ("ewma", np.float64), ("size", np.int64))


def _write_cache(rows, cache: Path, chunk: int = 1 << 16) -> None:
    tmp = cache.with_name(cache.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    vocab: dict[str, int] = {}
    total = 0
    files = {name: open(tmp / f"{name}.bin", "wb") for name, _ in _COLUMNS}
    try:
        def fresh():
            return {name: np.empty((chunk, len(FEATURES)) if name == "x" else chunk, dtype=dtype)
                    for name, dtype in _COLUMNS}

        def flush(n):
            for name, _ in _COLUMNS:
                files[name].write(buf[name][:n].tobytes())

        buf, n = fresh(), 0
        for x, reads, at, split, version, ewma, size in rows:
            buf["x"][n] = x
            buf["reads"][n], buf["at"][n], buf["split"][n] = reads, at, split
            buf["version"][n] = vocab.setdefault(version, len(vocab))
            buf["ewma"][n], buf["size"][n] = ewma, size
            n += 1
            if n == chunk:
                flush(n)
                total, n = total + n, 0
        flush(n)
        total += n
    finally:
        for f in files.values():
            f.close()
    (tmp / "versions.json").write_text(json.dumps(list(vocab)), encoding="utf-8")
    (tmp / "meta.json").write_text(json.dumps({"rows": total}), encoding="utf-8")
    tmp.rename(cache)


def _fill(path: Path, out: np.ndarray) -> None:
    if out.size == 0:
        return
    view = memoryview(out).cast("B")
    with open(path, "rb") as f:
        while view:
            n = f.readinto(view)
            if not n:
                raise ValueError(f"{path}: truncated cache")
            view = view[n:]


def _read(caches: list[Path]) -> dict[str, np.ndarray]:
    """Columns of several caches, in order, read straight into the result."""
    lengths = [json.loads((c / "meta.json").read_text())["rows"] for c in caches]
    total = sum(lengths)
    cols = {}
    for name, dtype in _COLUMNS:
        out = np.empty((total, len(FEATURES)) if name == "x" else total, dtype=dtype)
        at = 0
        for c, n in zip(caches, lengths):
            _fill(c / f"{name}.bin", out[at:at + n])
            at += n
        cols[name] = out
    ids: dict[str, int] = {}
    at = 0
    for c, n in zip(caches, lengths):
        local = json.loads((c / "versions.json").read_text(encoding="utf-8"))
        remap = np.array([ids.setdefault(v, len(ids)) for v in local], dtype=np.int32)
        if n:
            cols["version"][at:at + n] = remap[cols["version"][at:at + n]]
        at += n
    cols["versions"] = np.array(list(ids), dtype=str)
    return cols


def _cache(source: Path, name: str, manifest: dict, rows) -> Path:
    digest = manifest["sha256"][name]
    stem = name.split(".")[0]
    cache = source / f"{stem}.{digest[:16]}.v{_CACHE_FORMAT}"
    if not cache.exists():
        _write_cache(rows(source / name), cache)
    return cache


def load(source: str | Path, *, include_test: bool = False, train_every: int | None = None) -> Slices:
    """Load eligible rows of a dataset directory.

    Test rows stay out unless include_test is set: stage 8 selects and reports
    on validation only. train_every thins train snapshots to every N seconds
    (from the train start); validation keeps every snapshot.
    """
    source = Path(source)
    manifest = read_manifest(source)
    if manifest.get("format", "jsonl") == "csv.gz":
        digest = hashlib.sha256((source / "manifest.json").read_bytes()).hexdigest()
        splits = SPLITS if include_test else SPLITS[:2]
        cols = _read([_cache(source, f"{s}.csv.gz", manifest, _compact_rows) for s in splits])
    else:
        cols = _read([_cache(source, "slices.jsonl", manifest, _jsonl_rows)])
        digest = manifest["sha256"]["slices.jsonl"]
    periods = periods_of(manifest)
    keep = np.ones(len(cols["reads"]), dtype=bool)
    if not include_test:
        keep &= cols["split"] != _SPLIT_CODE["test"]
    if train_every:
        offset = cols["at"] - periods.train[0]
        keep &= (cols["split"] != _SPLIT_CODE["train"]) | (offset % train_every == 0)
    s = Slices(source.name, cols["x"], cols["reads"], cols["at"], cols["split"], cols["version"],
               cols["ewma"], cols["size"], periods, manifest, digest, cols["versions"])
    # Feature arrays of the acceptance profile take ~0.75 GiB: copy only to drop rows.
    return s if keep.all() else s.take(keep)


def require_test(slices: Slices) -> Slices:
    rows = slices.of("test")
    if len(rows) == 0:
        raise TestSplitLocked("test rows are not loaded; stage 8 never opens the test period")
    return rows
