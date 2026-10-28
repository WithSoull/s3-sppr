import argparse
import json
import subprocess
import sys
from types import SimpleNamespace

import numpy as np
import pytest

from sppr_ml import baselines, data, final_evaluation, model
from test_data import row, write_compact, write_dataset


def test_known_test_metrics_and_hourly_units(tmp_path):
    write_dataset(tmp_path / "d", [row("2026-10-04T00:00:00Z", "test", n) for n in [1, 0, 2, 0]])
    rows = data.load(tmp_path / "d", include_test=True)
    rows.manifest["horizon_seconds"] = 1800
    s = final_evaluation.scores(rows, np.array([1.8, 1.6, 1.4, 0.2]), np.array([0.9, 0.8, 0.7, 0.1]))
    assert s["pr_auc"] == pytest.approx(5 / 6)
    assert s["brier"] == pytest.approx(0.1875)
    assert s["mae_reads_per_hour"] == pytest.approx(1.6)
    assert s["rows"] == 4 and s["cov_k"] == 0  # floor(4 * 0.0921) = 0


def test_cov_k_uses_stage8_share_per_snapshot(tmp_path):
    write_dataset(tmp_path / "d", [row("2026-10-04T00:00:00Z", "test", n) for n in [10, 5] + [0] * 18])
    rows = data.load(tmp_path / "d", include_test=True)
    s = final_evaluation.scores(rows, np.arange(20, 0, -1), np.full(20, 0.5))
    assert s["cov_k"] == pytest.approx(10 / 15)  # K=1, not ceil or rounded K


def test_ewma_probability_rule():
    lam = np.array([0, np.log(2), 1, 20])
    np.testing.assert_allclose(baselines.probability(lam), [0, 0.5, 1 - np.exp(-1), 1 - np.exp(-20)])
    assert "1 - exp(-lambda)" in final_evaluation.DEFINITIONS["ewma_probability"]


def test_only_train_reaches_fit_and_test_is_opened_after_fit(tmp_path, monkeypatch):
    rows = [row("2026-10-01T00:00:00Z", "train", 1),
            row("2026-10-01T00:05:00Z", "train", 999),  # train thinning matches fit-only
            row("2026-10-01T00:15:00Z", "train", 3),
            row("2026-10-03T00:00:00Z", "validation", 888),
            row("2026-10-04T00:00:00Z", "test", 777)]
    write_dataset(tmp_path / "j", rows)
    write_compact(tmp_path / "c", rows)
    # Validation is never used by final evaluation, even for early stopping.
    (tmp_path / "c" / "validation.csv.gz").unlink()
    params = {"num_leaves": 7}
    calls = []

    def fit(train, fixed):
        calls.append(True)
        assert fixed == params
        assert train.reads.tolist() == [1, 3]
        assert train.split.tolist() == [0, 0]
        assert not list((tmp_path / "c").glob("test.*.v2"))
        return SimpleNamespace(rounds={"classifier": 1}, predict=lambda x: model.Forecast(np.full(len(x), 0.25), np.ones(len(x))))

    monkeypatch.setattr(model, "fit", fit)
    args = argparse.Namespace(data=tmp_path / "c", params=json.dumps(params), out=tmp_path / "out")
    final_evaluation.command(args)
    result = json.loads((tmp_path / "out" / "evaluate.json").read_text())
    assert len(calls) == 1 and result["train_rows"] == 2
    assert result["scores"]["ml"]["rows"] == 1
    assert result["scores"]["ml"]["mae_reads_per_hour"] == pytest.approx(776.5)
    assert result["scores"]["ewma"]["brier"] == pytest.approx(np.exp(-1))  # lambda=0.5
    with pytest.raises(ValueError, match="already exists"):
        final_evaluation.command(args)
    with pytest.raises(data.TestSplitLocked):
        data.load(tmp_path / "c", splits=("test",))


def test_empty_and_zero_demand_test(tmp_path):
    write_dataset(tmp_path / "d", [row("2026-10-04T00:00:00Z", "test", 0)])
    rows = data.load(tmp_path / "d", include_test=True)
    scores = final_evaluation.scores(rows, np.array([0.0]), np.array([0.0]))
    assert scores == {"pr_auc": None, "brier": 0, "mae_reads_per_hour": 0, "cov_k": None, "rows": 1}
    with pytest.raises(ValueError, match="no eligible test"):
        final_evaluation.scores(rows.of("train"), np.array([]), np.array([]))


def test_report_seed_means_and_ranges(tmp_path, capsys):
    dirs = []
    for i, v in enumerate([1.0, 3.0]):
        d = tmp_path / str(i)
        d.mkdir()
        scores = {k: v for k in final_evaluation.METRICS}
        r = {"schema": "forecast-quality-v1", "split": "test", "params": {}, "horizon_seconds": 3600,
             "definitions": final_evaluation.DEFINITIONS, "periods": {}, "scores": {"ewma": scores, "ml": scores}}
        (d / "evaluate.json").write_text(json.dumps(r))
        dirs.append(d)
    final_evaluation.report(argparse.Namespace(dirs=dirs))
    output = capsys.readouterr().out
    assert "2 seeds" in output
    assert "| pr_auc | 2 [1–3] | 2 [1–3] |" in output
    r["horizon_seconds"] = 1800
    (dirs[1] / "evaluate.json").write_text(json.dumps(r))
    with pytest.raises(ValueError, match="different horizon"):
        final_evaluation.report(argparse.Namespace(dirs=dirs))


def test_cli_real_train_only_fit_and_report(tmp_path):
    rows = []
    for split, at in [("train", "2026-10-01T00:00:00Z"), ("train", "2026-10-02T00:00:00Z"),
                      ("test", "2026-10-04T00:00:00Z")]:
        for i in range(20):
            r = row(at, split, 3 if i % 2 else 0)
            r["snapshot"]["version_id"] = f"v{i}"
            r["snapshot"]["features"]["reads_1h"] = i % 2
            rows.append(r)
    write_dataset(tmp_path / "d", rows)
    out = tmp_path / "out"
    subprocess.run([sys.executable, "-m", "sppr_ml", "evaluate", "--data", str(tmp_path / "d"),
                    "--params", '{"num_leaves":3,"min_data_in_leaf":1}', "--out", str(out)], check=True, capture_output=True)
    result = json.loads((out / "evaluate.json").read_text())
    assert result["train_rows"] == 40
    assert result["scores"]["ml"]["rows"] == 20
    assert 0 <= result["scores"]["ml"]["brier"] <= 1
    assert result["rounds"]["regressor"] > 0
    assert sorted(p.name for p in out.iterdir()) == ["evaluate.json"]
    reported = subprocess.run([sys.executable, "-m", "sppr_ml", "evaluate-report", str(out)],
                              check=True, capture_output=True, text=True)
    assert "| metric | EWMA | ML |" in reported.stdout
