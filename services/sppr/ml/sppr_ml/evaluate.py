"""Hyperparameter selection on validation and the comparison with baselines."""

from __future__ import annotations

import itertools
import time
from dataclasses import asdict, dataclass

import numpy as np

from . import baselines, metrics, model
from .data import Slices

# A small grid fixed before looking at results; the same for every source.
GRID = {
    "num_leaves": [15, 31, 63],
    "learning_rate": [0.03, 0.1],
    "min_data_in_leaf": [20, 200],
    "lambda_l2": [0.0, 1.0],
}
# Coverage closer than this counts as a tie, decided by Brier.
COVERAGE_TIE = 0.001


def grid() -> list[dict]:
    keys = list(GRID)
    return [dict(zip(keys, values)) for values in itertools.product(*(GRID[k] for k in keys))]


@dataclass
class Trial:
    params: dict
    rounds: dict
    scores: metrics.Scores
    seconds: float


def better(a: Trial, b: Trial) -> bool:
    if abs(a.scores.coverage - b.scores.coverage) > COVERAGE_TIE:
        return a.scores.coverage > b.scores.coverage
    return a.scores.brier < b.scores.brier


def tune(train: Slices, validation: Slices, candidates: list[dict] | None = None,
         log=print) -> tuple[model.Hurdle, list[Trial]]:
    best_model, best, trials = None, None, []
    for params in candidates or grid():
        start = time.perf_counter()
        hurdle = model.fit(train, params)
        f = hurdle.predict(validation.x)
        trial = Trial(params, hurdle.rounds, metrics.score(validation.at, validation.reads, f.expected, f.p),
                      time.perf_counter() - start)
        trials.append(trial)
        log(f"{params} rounds={hurdle.rounds} cov={trial.scores.coverage:.4f} brier={trial.scores.brier:.4f} {trial.seconds:.1f}s")
        if best is None or better(trial, best):
            best_model, best = hurdle, trial
    return best_model, trials


def _baselines(train: Slices | None, fitted_baselines: dict | None) -> dict:
    return baselines.fitted(train) if fitted_baselines is None else fitted_baselines


def compare(hurdle: model.Hurdle, train: Slices | None, rows: Slices, *,
            fitted_baselines: dict | None = None) -> dict[str, metrics.Scores]:
    """Scores on any evaluation split, using baselines fitted on train only."""
    f = hurdle.predict(rows.x)
    out = {"lgbm": metrics.score(rows.at, rows.reads, f.expected, f.p)}
    for name, estimate in _baselines(train, fitted_baselines).items():
        e = estimate(rows)
        out[name] = metrics.score(rows.at, rows.reads, e, baselines.probability(e))
    return out


@dataclass
class Difference:
    baseline: str
    coverage: float
    low: float
    high: float


def versus_baselines(hurdle: model.Hurdle, train: Slices | None, rows: Slices, *,
                     fitted_baselines: dict | None = None) -> dict[str, Difference]:
    """Paired Cov_K differences on the same snapshot bootstrap samples."""
    f = hurdle.predict(rows.x)
    caught_m, _, total = metrics.coverage_parts(rows.at, rows.reads, f.expected)
    out = {}
    for name, estimate in _baselines(train, fitted_baselines).items():
        caught_b, _, _ = metrics.coverage_parts(rows.at, rows.reads, estimate(rows))
        out[name] = Difference(name, *metrics.paired_difference(caught_m, caught_b, total))
    return out


def versus_best_baseline(hurdle: model.Hurdle, train: Slices | None, rows: Slices,
                         scores: dict[str, metrics.Scores], *, fitted_baselines: dict | None = None) -> Difference:
    """Paired Cov_K difference against the best baseline on the evaluation split."""
    best = max((k for k in scores if k != "lgbm"), key=lambda k: scores[k].coverage)
    estimates = _baselines(train, fitted_baselines)
    return versus_baselines(hurdle, None, rows, fitted_baselines={best: estimates[best]})[best]


@dataclass
class NeverRead:
    read_share: float
    without: Difference


def never_read_check(hurdle: model.Hurdle, train: Slices | None, rows: Slices, *,
                     fitted_baselines: dict | None = None) -> NeverRead:
    """How much of the result rests on objects never read before the snapshot.

    A converter that infers the catalogue from the trace (IBM COS: an object
    first seen later is assumed to exist from the start) puts an object in the
    catalogue because it will be touched — a leak a real catalogue lacks. Every
    estimate is zeroed on never-read rows and the paired difference recomputed.
    """
    never = np.isnan(rows.feature("last_read_seconds"))
    reads = rows.reads
    estimates = {"lgbm": hurdle.predict(rows.x).expected,
                 **{k: f(rows) for k, f in _baselines(train, fitted_baselines).items()}}
    parts = {k: metrics.coverage_parts(rows.at, reads, np.where(never, 0.0, e)) for k, e in estimates.items()}
    best = max((k for k in parts if k != "lgbm"), key=lambda k: parts[k][0].sum())
    total = parts["lgbm"][2]
    return NeverRead(float(reads[never].sum() / reads.sum()) if reads.sum() else 0.0,
                     Difference(best, *metrics.paired_difference(parts["lgbm"][0], parts[best][0], total)))


def as_dict(scores: dict[str, metrics.Scores]) -> dict[str, dict]:
    return {k: asdict(v) for k, v in scores.items()}


def label_summary(rows: Slices) -> dict:
    r = rows.reads
    return {
        "rows": int(len(r)),
        "zero_share": float(np.mean(r == 0)),
        "mean": float(r.mean()),
        "p50": float(np.percentile(r, 50)),
        "p90": float(np.percentile(r, 90)),
        "p99": float(np.percentile(r, 99)),
        "max": float(r.max()),
    }
