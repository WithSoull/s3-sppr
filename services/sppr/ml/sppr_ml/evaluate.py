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


def compare(hurdle: model.Hurdle, train: Slices, validation: Slices) -> dict[str, metrics.Scores]:
    """Validation scores of the model and every baseline fitted on train."""
    f = hurdle.predict(validation.x)
    out = {"lgbm": metrics.score(validation.at, validation.reads, f.expected, f.p)}
    for name, estimate in baselines.fitted(train).items():
        e = estimate(validation)
        out[name] = metrics.score(validation.at, validation.reads, e, baselines.probability(e))
    return out


@dataclass
class Difference:
    baseline: str
    coverage: float
    low: float
    high: float


def versus_best_baseline(hurdle: model.Hurdle, train: Slices, validation: Slices,
                         scores: dict[str, metrics.Scores]) -> Difference:
    """Paired difference in Cov_K between the model and the best baseline on validation."""
    best = max((k for k in scores if k != "lgbm"), key=lambda k: scores[k].coverage)
    f = hurdle.predict(validation.x)
    caught_m, _, total = metrics.coverage_parts(validation.at, validation.reads, f.expected)
    caught_b, _, _ = metrics.coverage_parts(validation.at, validation.reads, baselines.fitted(train)[best](validation))
    return Difference(best, *metrics.paired_difference(caught_m, caught_b, total))


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
