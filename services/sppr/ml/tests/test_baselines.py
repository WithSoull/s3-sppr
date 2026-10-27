import numpy as np
import pytest

from sppr_ml.baselines import MonotoneMap, _pav, probability


def test_pav_merges_violations():
    assert _pav(np.array([1.0, 3.0, 2.0, 4.0]), np.ones(4)).tolist() == pytest.approx([1, 2.5, 2.5, 4])


def test_monotone_map_follows_decreasing_relation_and_keeps_missing_apart():
    rng = np.random.default_rng(0)
    recency = rng.uniform(0, 3600, 5000)
    reads = np.maximum(0, 5 - recency / 720 + rng.normal(0, 0.5, 5000))
    feature = np.r_[recency, [np.nan] * 100]
    m = MonotoneMap.fit(feature, np.r_[reads, np.zeros(100)])
    out = m(np.array([10.0, 1800.0, 3590.0, np.nan]))
    assert out[0] > out[1] > out[2]
    assert out[3] == 0.0


def test_probability_of_poisson():
    assert probability(np.array([0.0, np.log(2)])) == pytest.approx([0.0, 0.5])
