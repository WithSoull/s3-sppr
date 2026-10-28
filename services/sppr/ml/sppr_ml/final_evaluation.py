"""Stage 11: fixed-parameter train-only fit, followed by final TEST evaluation."""

from __future__ import annotations

import json
from pathlib import Path

import lightgbm as lgb
import numpy as np

from . import baselines, data, metrics, model

METRICS = ("pr_auc", "brier", "mae_reads_per_hour", "cov_k", "rows")
DEFINITIONS = {
    "pr_auc": "average precision; equal scores form one threshold",
    "event": ">=1 read in horizon",
    "ewma_probability": "1 - exp(-lambda); lambda = EWMA expected reads over the horizon",
    "mae_reads_per_hour": "mean(abs(expected_reads - label_reads)) * 3600 / horizon_seconds",
    "cov_k": "sum(caught reads) / sum(all reads); top K per snapshot; K = floor(hot_share * objects)",
    "hot_share": metrics.HOT_SHARE,
    "tie_seed": 0,
    "undefined": "null when there are no positive labels (PR-AUC/Cov_K)",
}


def scores(rows: data.Slices, expected: np.ndarray, probability: np.ndarray) -> dict:
    if not len(rows):
        raise ValueError("no eligible test rows")
    result = {
        "pr_auc": metrics.average_precision(rows.reads >= 1, probability),
        "brier": metrics.brier(rows.reads >= 1, probability),
        "mae_reads_per_hour": float(np.mean(np.abs(expected - rows.reads)) * 3600 / rows.manifest["horizon_seconds"]),
        "cov_k": metrics.coverage(rows.at, rows.reads, expected),
        "rows": len(rows),
    }
    return {k: None if isinstance(v, float) and not np.isfinite(v) else v for k, v in result.items()}


def run(source: str | Path, params: dict) -> dict:
    if not isinstance(params, dict):
        raise ValueError("--params must be a JSON object of fixed hyperparameters")
    # Same fit and train thinning as train --params ... --fit-only. No tuning
    # or validation comparison; test is opened only after fitting is finished.
    train = data.load(source, splits=("train",), train_every=900)
    hurdle = model.fit(train, params)
    train_count = len(train)
    del train
    test = data.load(source, splits=("test",), include_test=True)
    if not len(test):
        raise ValueError("no eligible test rows")
    forecast = hurdle.predict(test.x)
    ewma = baselines.ewma(test)
    manifest = test.manifest
    return {
        "schema": "forecast-quality-v1", "split": "test", "source": test.source,
        "config": manifest.get("config"), "provenance": manifest.get("provenance"),
        "data_sha256": test.data_sha256,
        "dataset_manifest": manifest,
        "versions": {"lightgbm": lgb.__version__, "numpy": np.__version__},
        "periods": {s: manifest[s] for s in data.SPLITS},
        "params": params, "rounds": hurdle.rounds, "train_rows": train_count,
        "train_every_seconds": 900, "horizon_seconds": manifest["horizon_seconds"],
        "selection": "fixed params; early stopping on train tail only; no validation/test tuning",
        "definitions": DEFINITIONS,
        "scores": {"ewma": scores(test, ewma, baselines.probability(ewma)),
                   "ml": scores(test, forecast.expected, forecast.p)},
    }


def command(args) -> None:
    out = Path(args.out)
    if out.exists():
        raise ValueError(f"output already exists: {out}")
    result = run(args.data, json.loads(args.params))
    out.mkdir(parents=True, exist_ok=False)
    (out / "evaluate.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(out / "evaluate.json")


def report(args) -> None:
    reports = [json.loads((Path(d) / "evaluate.json").read_text(encoding="utf-8")) for d in args.dirs]
    for r in reports:
        if r["schema"] != "forecast-quality-v1" or r["split"] != "test":
            raise ValueError("expected stage 11 TEST evaluations")
        for key in ("params", "horizon_seconds", "definitions", "periods"):
            if r[key] != reports[0][key]:
                raise ValueError(f"cannot average evaluations with different {key}")
        config = {k: v for k, v in (r.get("config") or {}).items() if k != "seed"}
        first_config = {k: v for k, v in (reports[0].get("config") or {}).items() if k != "seed"}
        if config != first_config:
            raise ValueError("cannot average different scenarios/configs; pass seeds of one scenario")

    def fmt(values):
        if any(v is None for v in values):
            return "— (undefined in at least one seed)"
        return f"{np.mean(values):.6g} [{min(values):.6g}–{max(values):.6g}]"

    print(f"{len(reports)} seeds; mean [min–max]; TEST. Equal weight per seed.\n")
    print("| metric | EWMA | ML |")
    print("|---|---|---|")
    for metric in METRICS:
        ewma, ml = (fmt([r["scores"][policy][metric] for r in reports]) for policy in ("ewma", "ml"))
        print(f"| {metric} | {ewma} | {ml} |")
