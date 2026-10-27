"""Model directory for SPPR: both boosters, manifest.json and parity.json.

parity.json keeps the Python outputs of control rows, so the Go predictor can
prove it computes the same p, m and N̂. Control rows come from train and
validation only; the test period stays closed in stage 8.
"""

from __future__ import annotations

import hashlib
import json
import math
import platform
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import scipy

from . import data
from .data import FEATURES, NULLABLE, Slices
from .model import BASE_PARAMS, Hurdle

METHOD_VERSION = "lgbm-hurdle-v1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _features(row: np.ndarray) -> dict:
    return {n: (None if math.isnan(v) else float(v)) for n, v in zip(FEATURES, row)}


def _split_points(booster: lgb.Booster) -> list[tuple[str, float]]:
    points = []

    def walk(node):
        if "split_feature" not in node:
            return
        if node["decision_type"] == "<=":
            points.append((FEATURES[node["split_feature"]], node["threshold"]))
        walk(node["left_child"])
        walk(node["right_child"])

    for tree in booster.dump_model()["tree_info"]:
        walk(tree["tree_structure"])
    return points


def control_rows(rows: Slices, hurdle: Hurdle) -> list[dict]:
    """Real rows of every kind, out-of-range inputs and inputs exactly at split thresholds."""
    out, picked = [], set()
    rng = np.random.default_rng(0)

    def take(name, idx, limit):
        for i in idx[:limit]:
            key = (str(rows.version[i]), int(rows.at[i]))
            if key in picked:
                continue
            picked.add(key)
            out.append({"name": f"{name}:{key[0]}@{_iso(key[1])}", "features": _features(rows.x[i])})

    last = rows.feature("last_read_seconds")
    prev = rows.feature("previous_read_seconds")
    take("zero_never_read", np.flatnonzero((rows.reads == 0) & np.isnan(last)), 6)
    take("zero_with_history", np.flatnonzero((rows.reads == 0) & ~np.isnan(last)), 6)
    take("positive_top", np.argsort(-rows.reads, kind="stable"), 8)
    take("one_previous_read", np.flatnonzero(~np.isnan(last) & np.isnan(prev)), 4)
    take("random", rng.choice(len(rows), size=min(12, len(rows)), replace=False), 12)

    zero = {n: 0.0 for n in FEATURES}
    out.append({"name": "synthetic:all_zero", "features": zero})
    out.append({"name": "synthetic:all_nullable_missing", "features": {**zero, **{n: None for n in NULLABLE}}})
    out.append({"name": "synthetic:huge", "features": {n: 1e12 for n in FEATURES}})
    out.append({"name": "synthetic:tiny", "features": {n: 5e-324 for n in FEATURES}})

    base = _features(rows.x[0])
    seen = set()
    for model_name, booster in (("classifier", hurdle.classifier), ("regressor", hurdle.regressor)):
        for name, t in _split_points(booster)[:20]:
            if (name, t) in seen:
                continue
            seen.add((name, t))
            for label, v in (("at", t), ("below", math.nextafter(t, -math.inf)), ("above", math.nextafter(t, math.inf))):
                out.append({"name": f"threshold:{model_name}:{name}:{label}", "features": {**base, name: v}})
    return out


def _matrix(controls: list[dict]) -> np.ndarray:
    return np.array([[math.nan if c["features"][n] is None else c["features"][n] for n in FEATURES] for c in controls])


def write(out: Path, hurdle: Hurdle, train: Slices, validation: Slices, scores: dict, trials: list) -> dict:
    out.mkdir(parents=True, exist_ok=False)
    hurdle.classifier.save_model(out / "classifier.txt")
    hurdle.regressor.save_model(out / "regressor.txt")

    controls = control_rows(validation if len(validation) else train, hurdle)
    x = _matrix(controls)
    p, excess = hurdle.classifier.predict(x), hurdle.regressor.predict(x)
    raw_cls, raw_reg = hurdle.classifier.predict(x, raw_score=True), hurdle.regressor.predict(x, raw_score=True)
    for i, c in enumerate(controls):
        c["want"] = {
            "classifier_raw": float(raw_cls[i]), "probability": float(p[i]),
            "regressor_raw": float(raw_reg[i]), "excess_reads": float(excess[i]),
            "conditional_reads": 1.0 + float(excess[i]), "expected_reads": float(p[i]) * (1.0 + float(excess[i])),
        }
    with open(out / "parity.json", "w", encoding="utf-8") as f:
        json.dump({"meta": {"features": FEATURES, "rules_version": data.RULES_VERSION}, "rows": controls},
                  f, indent=1, allow_nan=False)
        f.write("\n")

    files = {name: _sha256(out / name) for name in ("classifier.txt", "regressor.txt", "parity.json")}
    # Field names follow "Схема файлов экспорта v1" in docs/ml-spec.md; Go validates them.
    manifest = {
        "model_version": f"{METHOD_VERSION}-{train.source}-{files['classifier.txt'][:8]}{files['regressor.txt'][:8]}",
        "method_version": METHOD_VERSION,
        "feature_rules_version": data.RULES_VERSION,
        "feature_names": FEATURES,
        "horizon_seconds": data.HORIZON_SECONDS,
        "lightgbm_version": lgb.__version__,
        "python_version": platform.python_version(),
        "numpy_version": np.__version__,
        "scipy_version": scipy.__version__,
        "hyperparameters": {**BASE_PARAMS, **hurdle.params, "rounds": hurdle.rounds},
        "train_interval": {"start": _iso(train.periods.train[0]), "end": _iso(train.periods.train[1]), "rows": len(train)},
        "validation_interval": {"start": _iso(validation.periods.validation[0]),
                                "end": _iso(validation.periods.validation[1]), "rows": len(validation)},
        "source": train.source,
        "source_config": train.manifest.get("config"),
        "source_provenance": train.manifest.get("provenance"),
        "data_sha256": train.manifest["sha256"]["slices.jsonl"],
        "validation_metrics": scores,
        "trials": len(trials),
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "classifier_sha256": files["classifier.txt"],
        "regressor_sha256": files["regressor.txt"],
        "parity_sha256": files["parity.json"],
    }
    with open(out / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, allow_nan=False)
        f.write("\n")
    return manifest
