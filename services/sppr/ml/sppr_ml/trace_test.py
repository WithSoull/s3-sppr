"""Final TEST evaluation of a model selected by the stage 8 validation grid."""

from __future__ import annotations

import gc
import json
import math
import platform
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

import lightgbm as lgb
import numpy as np

from . import baselines, data, evaluate, final_evaluation

SELECTION = "selected on validation (stage 8 grid); test opened once after selection"
BEST_BASELINE = "best baseline chosen by test Cov_K; conservative comparison, not model selection"


def _clean(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    return value


def run(source: str | Path, params: dict | None = None, train_every: int = 900) -> dict:
    if params is not None and not isinstance(params, dict):
        raise ValueError("--params must be a JSON object of fixed hyperparameters")
    # Exactly the train command's loading, thinning, grid and tie-breaking.
    rows = data.load(source, train_every=train_every)
    train, validation = rows.of("train"), rows.of("validation")
    del rows
    if not len(train) or not len(validation):
        raise ValueError("no eligible train/validation rows")
    hurdle, trials = evaluate.tune(train, validation, None if params is None else [params],
                                   log=lambda m: print(m, file=sys.stderr))
    estimates = baselines.fitted(train)
    train_count, validation_count = len(train), len(validation)
    selection_sha = train.data_sha256
    # Fitted baseline closures retain only small monotone maps, not train rows.
    del train, validation
    gc.collect()
    test = data.load(source, include_test=True, splits=("test",))
    if not len(test):
        raise ValueError("no eligible test rows")
    if test.data_sha256 != selection_sha:
        raise ValueError("dataset changed between selection and TEST evaluation")
    compared = evaluate.compare(hurdle, None, test, fitted_baselines=estimates)
    differences = evaluate.versus_baselines(hurdle, None, test, fitted_baselines=estimates)
    best = max(differences, key=lambda k: compared[k].coverage)
    never = evaluate.never_read_check(hurdle, None, test, fitted_baselines=estimates)
    forecast = hurdle.predict(test.x)
    ewma = estimates["ewma"](test)
    manifest = test.manifest
    result = {
        "schema": "trace-test-v1", "split": "test", "source": test.source,
        "objects": (manifest.get("source_manifest") or {}).get("counts", {}).get("versions"),
        "test_objects": int(len(np.unique(test.version))),
        "objects_definition": "catalogue versions from source_manifest.counts.versions; test_objects counts eligible TEST versions",
        "dataset_manifest": manifest, "provenance": manifest.get("provenance"),
        "source_manifest": manifest.get("source_manifest"), "data_sha256": test.data_sha256,
        "versions": {"python": platform.python_version(), "lightgbm": lgb.__version__, "numpy": np.__version__},
        "selection": SELECTION,
        "selection_candidates": "stage 8 full grid" if params is None else "fixed --params; one validation trial",
        "baseline_selection": BEST_BASELINE,
        "best": {"params": hurdle.params, "rounds": hurdle.rounds},
        "trials": [asdict(t) for t in trials],
        "train_every_seconds": train_every, "train_rows": train_count, "validation_rows": validation_count,
        "horizon_seconds": manifest["horizon_seconds"],
        "labels": {"test": evaluate.label_summary(test)},
        "scores": evaluate.as_dict(compared),
        "versus_best_baseline": asdict(differences[best]),
        "versus_baselines": {name: asdict(d) for name, d in differences.items()},
        "never_read": asdict(never),
        "forecast_scores": {
            "lgbm": final_evaluation.scores(test, forecast.expected, forecast.p),
            "ewma": final_evaluation.scores(test, ewma, baselines.probability(ewma)),
        },
        "definitions": {**final_evaluation.DEFINITIONS,
                        "paired_ci": "95% paired bootstrap over snapshot times; 1000 resamples, seed 0",
                        "never_read": "read_share on NaN last_read_seconds; without zeroes all methods' forecasts on those rows, preserving catalogue/K and labels; best baseline reselected",
                        "objects": "catalogue versions; dash if source manifest does not provide the count"},
    }
    if not test.reads.sum():
        # Coverage and its CI are undefined without any observed reads.
        for score in result["scores"].values():
            score["coverage_ci"] = (None, None)
        for diff in [result["versus_best_baseline"], result["never_read"]["without"], *result["versus_baselines"].values()]:
            diff["low"] = diff["high"] = None
    return _clean(result)


def command(args) -> None:
    out = Path(args.out)
    if out.exists():
        raise ValueError(f"output already exists: {out}")
    result = run(args.data, None if args.params is None else json.loads(args.params), args.train_every)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Publish a complete directory, so interrupted batch runs can safely resume.
    with tempfile.TemporaryDirectory(prefix=f".{out.name}-", dir=out.parent) as tmp:
        (Path(tmp) / "test-report.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        Path(tmp).rename(out)
    print(out / "test-report.json")


def verdict(difference: dict, source: str = "") -> str:
    if source == "ibm-cos-001":
        return "не засчитано (утечка каталога)"
    low, high = difference["low"], difference["high"]
    if low is not None and math.isfinite(low) and low > 0:
        return "лучше"
    if high is not None and math.isfinite(high) and high < 0:
        return "хуже"
    return "не доказано"


def report(args) -> None:
    def fmt(value):
        return "—" if value is None else f"{value:.4f}"

    def diff(value):
        return f"{fmt(value['coverage'])} [{fmt(value['low'])}, {fmt(value['high'])}]"

    print("TEST; лучшая базовая выбрана по TEST Cov_K (консервативное сравнение). "
          "95 % ДИ: парный bootstrap по моментам среза. Объекты = версии каталога.\n")
    print("| source | objects | best baseline (test) | Cov_K model | Cov_K baseline | difference [95% CI] "
          "| verdict | never_read share | difference without never-read [CI] | Brier model/EWMA | MAE model/EWMA (reads/object/hour) |")
    print("|---|---:|---|---:|---:|---|---|---:|---|---|---|")
    for directory in args.dirs:
        r = json.loads((Path(directory) / "test-report.json").read_text(encoding="utf-8"))
        if r["schema"] != "trace-test-v1" or r["split"] != "test":
            raise ValueError("expected trace-test-v1 TEST evaluation")
        d, n = r["versus_best_baseline"], r["never_read"]
        ml, ewma = r["forecast_scores"]["lgbm"], r["forecast_scores"]["ewma"]
        share = "—" if n["read_share"] is None else f"{n['read_share']:.1%}"
        objects = r["objects"] if r["objects"] is not None else "—"
        print(f"| {r['source']} | {objects} | {d['baseline']} | {fmt(r['scores']['lgbm']['coverage'])} "
              f"| {fmt(r['scores'][d['baseline']]['coverage'])} | {diff(d)} | {verdict(d, r['source'])} "
              f"| {share} | {diff(n['without'])} ({n['without']['baseline']}) "
              f"| {fmt(ml['brier'])}/{fmt(ewma['brier'])} "
              f"| {fmt(ml['mae_reads_per_hour'])}/{fmt(ewma['mae_reads_per_hour'])} |")
