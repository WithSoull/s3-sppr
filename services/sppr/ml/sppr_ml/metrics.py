"""Forecast quality metrics of the thesis: PR-AUC, Brier and coverage Cov_K."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# HOT holds 921 of 10 000 objects in the acceptance profile (90 % of 1 GiB / 1 MiB).
HOT_SHARE = 0.0921


def average_precision(has_read: np.ndarray, score: np.ndarray) -> float:
    """PR-AUC as average precision: precision summed at every positive, ties ranked together."""
    y = np.asarray(has_read, dtype=bool)
    positives = int(y.sum())
    if positives == 0:
        return float("nan")
    order = np.argsort(-np.asarray(score, dtype=np.float64), kind="stable")
    s, y = np.asarray(score)[order], y[order]
    tp = np.cumsum(y)
    # Only the last row of a tie block is a valid threshold.
    last = np.r_[s[1:] != s[:-1], True]
    tp, rank = tp[last], (np.arange(1, len(y) + 1))[last]
    precision = tp / rank
    recall_step = np.diff(np.r_[0, tp]) / positives
    return float(np.sum(precision * recall_step))


def brier(has_read: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((np.asarray(p, dtype=np.float64) - np.asarray(has_read, dtype=np.float64)) ** 2))


def poisson_deviance(reads: np.ndarray, pred: np.ndarray) -> float:
    y = np.asarray(reads, dtype=np.float64)
    mu = np.maximum(np.asarray(pred, dtype=np.float64), 1e-9)
    term = np.where(y > 0, y * np.log(np.where(y > 0, y, 1) / mu), 0.0)
    return float(np.mean(2 * (term - (y - mu))))


def coverage_parts(at: np.ndarray, reads: np.ndarray, pred: np.ndarray, *, share: float = HOT_SHARE,
                   seed: int = 0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per snapshot time: reads caught by the top-K forecast, by the top-K actual reads, and all reads.

    K = floor(share · objects at that time). Equal forecasts are ordered randomly
    with a fixed seed, so a baseline full of zeros is not helped by file order.
    """
    rng = np.random.default_rng(seed)
    noise = rng.random(len(pred))
    order = np.lexsort((noise, -np.asarray(pred, dtype=np.float64), at))
    at_s, reads_s, pred_s = at[order], np.asarray(reads, dtype=np.float64)[order], pred[order]
    times, start = np.unique(at_s, return_index=True)
    caught, best, total = np.zeros(len(times)), np.zeros(len(times)), np.zeros(len(times))
    bounds = np.r_[start, len(at_s)]
    for i in range(len(times)):
        r = reads_s[bounds[i]:bounds[i + 1]]
        k = int(np.floor(share * len(r)))
        caught[i] = r[:k].sum()
        best[i] = np.sort(r)[::-1][:k].sum()
        total[i] = r.sum()
    return caught, best, total


def coverage(at: np.ndarray, reads: np.ndarray, pred: np.ndarray, *, share: float = HOT_SHARE) -> float:
    caught, _, total = coverage_parts(at, reads, pred, share=share)
    return float(caught.sum() / total.sum()) if total.sum() > 0 else float("nan")


@dataclass
class Scores:
    pr_auc: float
    brier: float
    coverage: float
    coverage_ci: tuple[float, float]
    oracle_coverage: float
    poisson_deviance: float
    mae: float


def bootstrap_ratio(caught: np.ndarray, total: np.ndarray, *, rounds: int = 1000, seed: int = 0) -> tuple[float, float]:
    """95 % interval of sum(caught)/sum(total) resampling snapshot times."""
    rng = np.random.default_rng(seed)
    n = len(total)
    idx = rng.integers(0, n, size=(rounds, n))
    ratios = caught[idx].sum(axis=1) / np.maximum(total[idx].sum(axis=1), 1e-12)
    lo, hi = np.percentile(ratios, [2.5, 97.5])
    return float(lo), float(hi)


def score(at: np.ndarray, reads: np.ndarray, expected: np.ndarray, p: np.ndarray, *, share: float = HOT_SHARE) -> Scores:
    has_read = reads > 0
    caught, best, total = coverage_parts(at, reads, expected, share=share)
    return Scores(
        pr_auc=average_precision(has_read, p),
        brier=brier(has_read, p),
        coverage=float(caught.sum() / total.sum()),
        coverage_ci=bootstrap_ratio(caught, total),
        oracle_coverage=float(best.sum() / total.sum()),
        poisson_deviance=poisson_deviance(reads, expected),
        mae=float(np.mean(np.abs(np.asarray(expected) - reads))),
    )


def paired_difference(caught_a: np.ndarray, caught_b: np.ndarray, total: np.ndarray, *, rounds: int = 1000,
                      seed: int = 0) -> tuple[float, float, float]:
    """Coverage of a minus coverage of b with a 95 % interval from the same resampled snapshot times."""
    rng = np.random.default_rng(seed)
    n = len(total)
    idx = rng.integers(0, n, size=(rounds, n))
    t = np.maximum(total[idx].sum(axis=1), 1e-12)
    diffs = (caught_a[idx].sum(axis=1) - caught_b[idx].sum(axis=1)) / t
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    return float((caught_a.sum() - caught_b.sum()) / total.sum()), float(lo), float(hi)
