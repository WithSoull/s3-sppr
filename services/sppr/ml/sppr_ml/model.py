"""Two-stage (hurdle) LightGBM of the thesis.

p = P(N >= 1) from a `binary` classifier on every row; the `poisson` regressor
learns y = N - 1 on rows with N >= 1, so m = 1 + ŷ and N̂ = p · m.

When train has no repeated read within the horizon (every positive label is 1,
as in IBM COS 001), y is all zero: LightGBM cannot fit a Poisson model to it and
the right estimate is ŷ = 0. The regressor is then left out (m = 1, N̂ = p);
such a model is evaluated but not exported, since the Go contract needs both.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import lightgbm as lgb
import numpy as np

from .data import FEATURES, Slices

TAIL_SECONDS = 8 * 3600
MAX_ROUNDS = 2000
EARLY_STOPPING = 50

# Fixed for every fit; tuned values are layered on top.
BASE_PARAMS = {
    "seed": 0,
    "deterministic": True,
    "force_row_wise": True,
    "num_threads": 2,
    "verbose": -1,
}


@dataclass
class Forecast:
    p: np.ndarray
    excess: np.ndarray

    @property
    def conditional(self) -> np.ndarray:
        return 1.0 + self.excess

    @property
    def expected(self) -> np.ndarray:
        return self.p * self.conditional


@dataclass
class Hurdle:
    classifier: lgb.Booster
    regressor: lgb.Booster | None
    params: dict
    rounds: dict = field(default_factory=dict)

    def predict(self, x: np.ndarray) -> Forecast:
        excess = np.zeros(len(x)) if self.regressor is None else self.regressor.predict(x)
        return Forecast(self.classifier.predict(x), excess)

    def contributions(self, x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Per-feature contributions in raw space (logit of p, log of ŷ); the last column is the bias."""
        if self.regressor is None:
            raise ValueError("no regressor: train had no repeated read, ŷ = 0")
        return self.classifier.predict(x, pred_contrib=True), self.regressor.predict(x, pred_contrib=True)


def _rounds(params: dict, objective: str, x: np.ndarray, y: np.ndarray, tail: np.ndarray) -> int:
    """Number of trees that early stopping on the last hours of train keeps."""
    if tail.sum() == 0 or (~tail).sum() == 0 or (objective == "poisson" and y[~tail].sum() == 0):
        return 100
    full = {**BASE_PARAMS, **params, "num_threads": 2, "objective": objective}
    booster = lgb.train(
        full,
        lgb.Dataset(x[~tail], label=y[~tail], feature_name=FEATURES),
        num_boost_round=MAX_ROUNDS,
        valid_sets=[lgb.Dataset(x[tail], label=y[tail], feature_name=FEATURES)],
        callbacks=[lgb.early_stopping(EARLY_STOPPING, verbose=False)],
    )
    return max(booster.best_iteration, 1)


def fit(train: Slices, params: dict) -> Hurdle:
    """Fit both stages on train rows only.

    The tree count of each stage comes from early stopping on the last
    TAIL_SECONDS of train; the final model is then refit on all of train with
    that count, so validation never takes part in fitting.
    """
    x, reads = train.x, train.reads
    tail = train.at >= train.periods.train[1] - TAIL_SECONDS
    has_read = (reads >= 1).astype(np.float64)
    positive = reads >= 1
    if positive.sum() == 0:
        raise ValueError("no positive rows in train: the regressor cannot be fitted")

    cls_rounds = _rounds(params, "binary", x, has_read, tail)
    classifier = lgb.train({**BASE_PARAMS, **params, "num_threads": 2, "objective": "binary"},
                           lgb.Dataset(x, label=has_read, feature_name=FEATURES), num_boost_round=cls_rounds)
    excess = reads[positive] - 1
    if excess.sum() == 0:
        return Hurdle(classifier, None, params, {"classifier": cls_rounds, "regressor": 0})
    reg_rounds = _rounds(params, "poisson", x[positive], excess, tail[positive])
    regressor = lgb.train({**BASE_PARAMS, **params, "num_threads": 2, "objective": "poisson"},
                          lgb.Dataset(x[positive], label=excess, feature_name=FEATURES),
                          num_boost_round=reg_rounds)
    return Hurdle(classifier, regressor, params, {"classifier": cls_rounds, "regressor": reg_rounds})
