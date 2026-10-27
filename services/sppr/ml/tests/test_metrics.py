import numpy as np
import pytest

from sppr_ml import metrics


@pytest.mark.parametrize(
    "y, score, want",
    [
        ([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1], (1 / 1 + 2 / 3) / 2),
        ([1, 1, 0], [0.9, 0.8, 0.1], 1.0),
        ([0, 1], [0.9, 0.1], 0.5),
        # A tie between a positive and a negative counts as one threshold.
        ([1, 0], [0.5, 0.5], 0.5),
    ],
)
def test_average_precision(y, score, want):
    assert metrics.average_precision(np.array(y), np.array(score)) == pytest.approx(want)


def test_average_precision_without_positives_is_nan():
    assert np.isnan(metrics.average_precision(np.array([0, 0]), np.array([0.1, 0.2])))


def test_brier():
    assert metrics.brier(np.array([1, 0]), np.array([0.8, 0.2])) == pytest.approx(0.04)


def test_coverage_counts_top_k_per_snapshot():
    # Two snapshots of 20 objects: K = floor(0.1 * 20) = 2.
    at = np.repeat([0, 900], 20)
    reads = np.zeros(40)
    reads[[0, 1, 2]] = [10, 5, 1]       # snapshot 0
    reads[[20, 21]] = [3, 3]            # snapshot 900
    pred = np.zeros(40)
    pred[[0, 2]] = [2, 1]               # misses object 1 at snapshot 0
    pred[[20, 21]] = [1, 1]
    caught, best, total = metrics.coverage_parts(at, reads, pred, share=0.1)
    assert caught.tolist() == [11, 6]
    assert best.tolist() == [15, 6]
    assert total.tolist() == [16, 6]
    assert metrics.coverage(at, reads, pred, share=0.1) == pytest.approx(17 / 22)


def test_coverage_breaks_ties_randomly_not_by_row_order():
    at = np.zeros(1000)
    reads = np.r_[np.ones(100), np.zeros(900)]
    pred = np.zeros(1000)
    # With every forecast equal, row order would give perfect coverage; random ties give about 10 %.
    assert metrics.coverage(at, reads, pred, share=0.1) < 0.3


def test_poisson_deviance_is_zero_for_exact_forecast():
    reads = np.array([0.0, 1.0, 4.0])
    assert metrics.poisson_deviance(reads, reads) == pytest.approx(0.0, abs=1e-6)


def test_paired_difference_sees_a_consistent_small_gain():
    total = np.full(200, 100.0)
    a = np.full(200, 51.0)
    b = np.full(200, 50.0)
    diff, lo, hi = metrics.paired_difference(a, b, total)
    assert diff == pytest.approx(0.01)
    assert lo > 0 and hi > 0
