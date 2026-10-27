"""Loading feature slices written by cmd/dataset.

Rows are kept as numpy columns. The parsed JSONL is cached next to it as .npz,
keyed by the slices SHA-256 from the manifest, because parsing hundreds of
megabytes of JSON dominates a notebook run otherwise.
"""

from __future__ import annotations

import json
import math
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
_CACHE_FORMAT = 1


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

    def __len__(self) -> int:
        return len(self.reads)

    def take(self, mask: np.ndarray) -> "Slices":
        return Slices(
            self.source, self.x[mask], self.reads[mask], self.at[mask], self.split[mask],
            self.version[mask], self.ewma[mask], self.size[mask], self.periods, self.manifest,
        )

    def of(self, split: str) -> "Slices":
        return self.take(self.split == _SPLIT_CODE[split])

    def feature(self, name: str) -> np.ndarray:
        return self.x[:, FEATURES.index(name)]


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


def _parse(path: Path) -> dict[str, np.ndarray]:
    x, reads, at, split, version, ewma, size = [], [], [], [], [], [], []
    with open(path, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if not row["eligible"]:
                continue
            snap = row["snapshot"]
            if snap["rules_version"] != RULES_VERSION:
                raise ValueError(f"unexpected rules version {snap['rules_version']}")
            feats = snap["features"]
            x.append([math.nan if feats[n] is None else float(feats[n]) for n in FEATURES])
            reads.append(row["label"]["reads"])
            at.append(_ts(snap["at"]))
            split.append(_SPLIT_CODE[row["split"]])
            version.append(snap["version_id"])
            ewma.append(row["forecast"]["expected_reads"])
            size.append(snap["size_bytes"])
    return {
        "x": np.asarray(x, dtype=np.float64).reshape(-1, len(FEATURES)),
        "reads": np.asarray(reads, dtype=np.float64),
        "at": np.asarray(at, dtype=np.int64),
        "split": np.asarray(split, dtype=np.int8),
        "version": np.asarray(version, dtype=str),
        "ewma": np.asarray(ewma, dtype=np.float64),
        "size": np.asarray(size, dtype=np.int64),
    }


def load(source: str | Path, *, include_test: bool = False, train_every: int | None = None) -> Slices:
    """Load eligible rows of a dataset directory.

    Test rows stay out unless include_test is set: stage 8 selects and reports
    on validation only. train_every thins train snapshots to every N seconds
    (from the train start); validation keeps every snapshot.
    """
    source = Path(source)
    manifest = read_manifest(source)
    digest = manifest["sha256"]["slices.jsonl"]
    cache = source / f"slices.{digest[:16]}.v{_CACHE_FORMAT}.npz"
    if cache.exists():
        with np.load(cache) as z:
            cols = {k: z[k] for k in z.files}
    else:
        cols = _parse(source / "slices.jsonl")
        np.savez(cache, **cols)
    periods = periods_of(manifest)
    keep = np.ones(len(cols["reads"]), dtype=bool)
    if not include_test:
        keep &= cols["split"] != _SPLIT_CODE["test"]
    if train_every:
        offset = cols["at"] - periods.train[0]
        keep &= (cols["split"] != _SPLIT_CODE["train"]) | (offset % train_every == 0)
    s = Slices(source.name, cols["x"], cols["reads"], cols["at"], cols["split"], cols["version"],
               cols["ewma"], cols["size"], periods, manifest)
    return s.take(keep)


def require_test(slices: Slices) -> Slices:
    rows = slices.of("test")
    if len(rows) == 0:
        raise TestSplitLocked("test rows are not loaded; stage 8 never opens the test period")
    return rows
