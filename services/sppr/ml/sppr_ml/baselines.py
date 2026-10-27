"""Baseline demand estimates of the thesis: EWMA, frequency, age and recency.

Each returns expected reads over the horizon. Age and recency are not counts,
so a non-increasing or non-decreasing step function fitted on train maps them
to expected reads. Probability of a read is taken as 1 - exp(-N̂) (Poisson).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.stats import spearmanr

from .data import HORIZON_SECONDS, Slices

BINS = 50


def probability(expected: np.ndarray) -> np.ndarray:
    return 1.0 - np.exp(-np.maximum(expected, 0.0))


def ewma(rows: Slices) -> np.ndarray:
    # cmd/dataset stores the EWMA forecast for H next to every slice.
    return rows.ewma


def frequency(rows: Slices) -> np.ndarray:
    return rows.feature("reads_24h") / 24.0 * (HORIZON_SECONDS / 3600.0)


def _pav(values: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Pool adjacent violators: the closest non-decreasing sequence in weighted least squares."""
    blocks = []
    for v, w in zip(values, weights):
        blocks.append([v * w, w, 1])
        while len(blocks) > 1 and blocks[-2][0] / blocks[-2][1] > blocks[-1][0] / blocks[-1][1]:
            s, w2, n = blocks.pop()
            blocks[-1][0] += s
            blocks[-1][1] += w2
            blocks[-1][2] += n
    out = []
    for s, w, n in blocks:
        out.extend([s / w] * n)
    return np.asarray(out)


@dataclass
class MonotoneMap:
    """Step function from a feature to expected reads; NaN (never read) gets its own value."""

    edges: np.ndarray
    values: np.ndarray
    missing: float

    @classmethod
    def fit(cls, feature: np.ndarray, reads: np.ndarray) -> "MonotoneMap":
        finite = np.isfinite(feature)
        f, r = feature[finite], reads[finite]
        missing = float(reads[~finite].mean()) if (~finite).any() else 0.0
        edges = np.unique(np.quantile(f, np.linspace(0, 1, BINS + 1)[1:-1])) if len(f) else np.array([])
        bins = np.searchsorted(edges, f, side="right")
        counts = np.bincount(bins, minlength=len(edges) + 1).astype(np.float64)
        sums = np.bincount(bins, weights=r, minlength=len(edges) + 1)
        means = np.divide(sums, counts, out=np.zeros_like(sums), where=counts > 0)
        weights = np.maximum(counts, 1e-9)
        # A constant feature (all objects of the same age) carries no direction.
        rho = spearmanr(f, r).statistic if len(f) > 1 and np.ptp(f) > 0 else 0.0
        if rho is not None and np.isfinite(rho) and rho < 0:
            values = -_pav(-means, weights)
        else:
            values = _pav(means, weights)
        return cls(edges, values, missing)

    def __call__(self, feature: np.ndarray) -> np.ndarray:
        out = np.full(len(feature), self.missing)
        finite = np.isfinite(feature)
        out[finite] = self.values[np.searchsorted(self.edges, feature[finite], side="right")]
        return out


def fitted(train: Slices) -> dict[str, callable]:
    """Every baseline as a function of rows, with the maps fitted on train."""
    age = MonotoneMap.fit(train.feature("age_seconds"), train.reads)
    recency = MonotoneMap.fit(train.feature("last_read_seconds"), train.reads)
    return {
        "ewma": ewma,
        "frequency": frequency,
        "age": lambda rows: age(rows.feature("age_seconds")),
        "recency": lambda rows: recency(rows.feature("last_read_seconds")),
    }
