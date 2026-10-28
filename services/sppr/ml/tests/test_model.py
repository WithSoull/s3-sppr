import json

import lightgbm as lgb
import numpy as np
import pytest

from sppr_ml import data, evaluate, export, model
from sppr_ml.data import FEATURES, Periods, Slices

HOUR = 3600


def synthetic(seed=0, objects=200, hours=72):
    """Hourly snapshots where reads depend on reads_1h, with zeros for cold objects."""
    rng = np.random.default_rng(seed)
    rate = np.where(np.arange(objects) < objects // 10, 6.0, 0.3)
    xs, reads, at, split = [], [], [], []
    last = rng.poisson(rate)
    for h in range(hours):
        now = rng.poisson(rate)
        x = np.zeros((objects, len(FEATURES)))
        x[:, FEATURES.index("reads_1h")] = last
        x[:, FEATURES.index("log_size")] = 13.86
        x[:, FEATURES.index("last_read_seconds")] = np.where(last > 0, 600.0, np.nan)
        xs.append(x); reads.append(now); at.append(np.full(objects, h * HOUR))
        split.append(np.full(objects, 0 if h < 48 else 1))
        last = now
    periods = Periods((0, 48 * HOUR), (48 * HOUR, 72 * HOUR), (72 * HOUR, 96 * HOUR))
    manifest = {"sha256": {"slices.jsonl": "0" * 64}, "config": {"scenario": "test"}}
    return Slices("test", np.vstack(xs), np.concatenate(reads).astype(float), np.concatenate(at),
                  np.concatenate(split).astype(np.int8), np.tile(np.arange(objects, dtype=np.int32), hours),
                  np.zeros(objects * hours), np.full(objects * hours, 1 << 20), periods, manifest, "0" * 64,
                  np.array([f"v{i}" for i in range(objects)]))


PARAMS = {"num_leaves": 7, "learning_rate": 0.1, "min_data_in_leaf": 20, "lambda_l2": 0.0}


def test_forecast_is_probability_times_conditional_count():
    f = model.Forecast(p=np.array([0.5, 0.0]), excess=np.array([3.0, 10.0]))
    assert f.conditional.tolist() == [4.0, 11.0]
    assert f.expected.tolist() == [2.0, 0.0]


def test_fit_learns_hot_objects_from_train_only():
    rows = synthetic()
    train, validation = rows.of("train"), rows.of("validation")
    hurdle = model.fit(train, PARAMS)
    f = hurdle.predict(validation.x)
    hot = validation.reads > 0
    assert f.expected[validation.feature("reads_1h") >= 4].mean() > 3 * f.expected[validation.feature("reads_1h") == 0].mean()
    assert 0 < f.p.min() and f.p.max() < 1
    assert (f.excess >= 0).all()
    assert hot.any()

    # Changing validation labels must not change the fitted model.
    other = validation.take(np.ones(len(validation), bool))
    other.reads = other.reads * 0
    again = model.fit(train, PARAMS).predict(other.x)
    np.testing.assert_allclose(again.expected, f.expected)


def test_export_round_trip_matches_parity(tmp_path):
    rows = synthetic()
    train, validation = rows.of("train"), rows.of("validation")
    hurdle = model.fit(train, PARAMS)
    scores = evaluate.as_dict(evaluate.compare(hurdle, train, validation))
    manifest = export.write(tmp_path / "m", hurdle, train, validation, scores, [])

    assert manifest["feature_names"] == FEATURES
    assert manifest["feature_rules_version"] == data.RULES_VERSION
    assert manifest["horizon_seconds"] == HOUR
    for field in ("lightgbm_version", "python_version", "numpy_version", "source", "data_sha256",
                  "classifier_sha256", "regressor_sha256"):
        assert manifest[field]
    for field in ("hyperparameters", "train_interval", "validation_interval", "validation_metrics"):
        assert isinstance(manifest[field], dict)
    cls = lgb.Booster(model_file=str(tmp_path / "m" / "classifier.txt"))
    reg = lgb.Booster(model_file=str(tmp_path / "m" / "regressor.txt"))
    parity = json.loads((tmp_path / "m" / "parity.json").read_text())
    names = {r["name"].split(":")[0] for r in parity["rows"]}
    assert {"zero_never_read", "positive_top", "synthetic"} <= names
    for row in parity["rows"]:
        x = np.array([[np.nan if row["features"][n] is None else row["features"][n] for n in FEATURES]])
        p, y = cls.predict(x)[0], reg.predict(x)[0]
        assert p == pytest.approx(row["want"]["probability"], rel=1e-12)
        assert p * (1 + y) == pytest.approx(row["want"]["expected_reads"], rel=1e-12)
        # The links Go applies explicitly: sigmoid for p, exp for ŷ.
        assert 1 / (1 + np.exp(-row["want"]["classifier_raw"])) == pytest.approx(p, rel=1e-12)
        assert np.exp(row["want"]["regressor_raw"]) == pytest.approx(y, rel=1e-12)


def test_test_split_is_locked_by_default():
    rows = synthetic()
    with pytest.raises(data.TestSplitLocked):
        data.require_test(rows)


def test_no_repeated_read_leaves_out_the_regressor(tmp_path):
    rows = synthetic()
    rows.reads = np.minimum(rows.reads, 1.0)
    hurdle = model.fit(rows.of("train"), PARAMS)
    assert hurdle.regressor is None and hurdle.rounds["regressor"] == 0
    f = hurdle.predict(rows.of("validation").x)
    np.testing.assert_array_equal(f.expected, f.p)
    with pytest.raises(ValueError, match="repeated read"):
        export.write(tmp_path / "m", hurdle, rows.of("train"), rows.of("validation"), {}, [])
    assert not (tmp_path / "m").exists()


def test_never_read_check_zeroes_rows_without_history():
    rows = synthetic()
    tr, va = rows.of("train"), rows.of("validation")
    hurdle = model.fit(tr, PARAMS)
    check = evaluate.never_read_check(hurdle, tr, va)
    never = np.isnan(va.feature("last_read_seconds"))
    assert check.read_share == pytest.approx(va.reads[never].sum() / va.reads.sum())
    assert check.without.low <= check.without.coverage <= check.without.high
