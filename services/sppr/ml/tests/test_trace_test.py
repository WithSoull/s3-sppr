import argparse
import importlib.util
import json
import subprocess
import sys
import weakref
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from sppr_ml import baselines, data, evaluate, model, trace_test
from test_data import row, write_compact, write_dataset


def dataset(tmp_path, test_reads=3):
    rows = []
    for split, at in [("train", "2026-10-01T00:00:00Z"), ("train", "2026-10-01T00:05:00Z"),
                      ("train", "2026-10-02T00:00:00Z"), ("validation", "2026-10-03T00:00:00Z"),
                      ("test", "2026-10-04T00:00:00Z")]:
        for i in range(20):
            r = row(at, split, (test_reads if split == "test" else 3) if i % 2 else 0,
                    last=None if i == 1 else 60.0)
            r["snapshot"]["features"]["reads_1h"] = i % 2
            rows.append(r)
    write_dataset(tmp_path / "j", rows)
    write_compact(tmp_path / "c", rows)
    path = tmp_path / "c" / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest.update(source_manifest={"counts": {"versions": 20}, "license": "fixture"}, provenance={"revision": "fixture"})
    path.write_text(json.dumps(manifest))
    return tmp_path / "c"


@pytest.mark.parametrize("params", [None, {"num_leaves": 3, "min_data_in_leaf": 1}])
def test_selection_precedes_test_load_and_releases_features(tmp_path, monkeypatch, params):
    source = dataset(tmp_path)
    calls, refs, predictions = [], [], []
    load, tune, fitted = data.load, evaluate.tune, baselines.fitted

    def recording_load(path, **kwargs):
        if kwargs.get("include_test"):
            assert calls[-1] == "fitted"
            assert all(ref() is None for ref in refs)
            assert kwargs == {"include_test": True, "splits": ("test",)}
        else:
            assert kwargs == {"train_every": 900}
        calls.append("test" if kwargs.get("include_test") else "train+validation")
        return load(path, **kwargs)

    def fit(train, candidate):
        assert set(train.split) == {0} and len(train) == 40
        calls.append("fit")
        f = model.Forecast(np.full(20, 0.25), np.ones(20))
        predictions.append(f)
        return SimpleNamespace(params=candidate, rounds={"classifier": 1, "regressor": 1}, predict=lambda x: f)

    def recording_tune(train, validation, candidates, **kwargs):
        assert len(validation) == 20 and set(validation.split) == {1}
        assert candidates == (None if params is None else [params])
        assert not list(source.glob("test.*.v2"))
        refs.extend([weakref.ref(train.x), weakref.ref(validation.x)])
        result = tune(train, validation, candidates, **kwargs)
        calls.append("selected")
        return result

    def recording_fitted(train):
        assert calls[-1] == "selected"
        calls.append("fitted")
        return fitted(train)

    monkeypatch.setattr(data, "load", recording_load)
    monkeypatch.setattr(evaluate, "tune", recording_tune)
    monkeypatch.setattr(model, "fit", fit)
    monkeypatch.setattr(baselines, "fitted", recording_fitted)
    result = trace_test.run(source, params)
    assert calls[-1] == "test"
    assert calls.count("fit") == (len(evaluate.grid()) if params is None else 1)
    assert result["schema"] == "trace-test-v1" and result["split"] == "test"
    assert result["selection"] == trace_test.SELECTION
    assert "test Cov_K" in result["baseline_selection"]
    assert result["best"]["params"] == (evaluate.grid()[0] if params is None else params)
    assert result["best"]["rounds"] == {"classifier": 1, "regressor": 1}
    assert len(result["trials"]) == calls.count("fit")
    assert {"params", "rounds", "scores", "seconds"} <= result["trials"][0].keys()
    assert result["objects"] == 20 and result["labels"]["test"]["rows"] == 20
    assert result["dataset_manifest"]["source_manifest"] == result["source_manifest"]
    assert result["provenance"] == {"revision": "fixture"} and len(result["data_sha256"]) == 64
    assert set(result["versions"]) == {"python", "lightgbm", "numpy"}
    assert set(result["scores"]) == {"lgbm", "ewma", "frequency", "age", "recency"}
    assert set(result["versus_baselines"]) == set(result["scores"]) - {"lgbm"}
    best = result["versus_best_baseline"]
    assert best == result["versus_baselines"][best["baseline"]]
    assert result["never_read"]["read_share"] == pytest.approx(0.1)
    ml, ewma = result["forecast_scores"]["lgbm"], result["forecast_scores"]["ewma"]
    assert ml["brier"] == pytest.approx(0.3125)
    assert ml["mae_reads_per_hour"] == pytest.approx(1.5)
    assert ewma["brier"] == pytest.approx(np.mean((1 - np.exp(-0.5) - np.array([0, 1])) ** 2))
    assert ewma["mae_reads_per_hour"] == pytest.approx(1.5)
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("low,high,want", [(0.01, 0.1, "лучше"), (-0.1, -0.01, "хуже"),
    (-0.1, 0.1, "не доказано"), (0, 0.1, "не доказано"), (-0.1, 0, "не доказано"),
    (None, None, "не доказано"), (float("nan"), float("nan"), "не доказано")])
def test_ci_verdict_and_cos_override(low, high, want):
    difference = {"low": low, "high": high}
    assert trace_test.verdict(difference) == want
    assert trace_test.verdict(difference, "ibm-cos-001") == "не засчитано (утечка каталога)"


def test_real_cli_fixed_selection_and_report(tmp_path):
    source = dataset(tmp_path)
    out = tmp_path / "out"
    command = [sys.executable, "-m", "sppr_ml", "test-eval", "--data", str(source),
               "--params", '{"num_leaves":3,"min_data_in_leaf":1}', "--out", str(out)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    r = json.loads((out / "test-report.json").read_text())
    assert r["train_rows"] == 40 and len(r["trials"]) == 1
    assert r["best"]["rounds"]["regressor"] > 0
    r["source"] = "ibm-cos-001"
    (out / "test-report.json").write_text(json.dumps(r))
    reported = subprocess.run([sys.executable, "-m", "sppr_ml", "test-eval-report", str(out)],
                              check=True, capture_output=True, text=True).stdout
    assert "| ibm-cos-001 | 20 |" in reported
    assert "не засчитано (утечка каталога)" in reported
    assert "Brier model/EWMA" in reported and "reads/object/hour" in reported
    assert subprocess.run(command, capture_output=True).returncode != 0
    assert sorted(p.name for p in out.iterdir()) == ["test-report.json"]


def test_zero_demand_and_invalid_params(tmp_path):
    source = dataset(tmp_path, test_reads=0)
    with pytest.raises(ValueError, match="JSON object"):
        trace_test.run(source, [])
    with np.errstate(invalid="ignore"):
        r = trace_test.run(source, {"num_leaves": 3, "min_data_in_leaf": 1})
    assert r["scores"]["lgbm"]["coverage"] is None
    assert r["scores"]["lgbm"]["coverage_ci"] == [None, None]
    assert trace_test.verdict(r["versus_best_baseline"]) == "не доказано"
    json.dumps(r, allow_nan=False)


def load_script(monkeypatch):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location("trace_test_script", scripts / "trace_test.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("keep", [False, True])
def test_batch_resumes_and_removes_only_datasets(tmp_path, monkeypatch, keep):
    script = load_script(monkeypatch)
    monkeypatch.setattr(script, "ROOT", tmp_path)
    monkeypatch.setattr(script.trace_datasets, "SOURCES", {"done": (), "new": ()})
    out = tmp_path / "build/ml-traces"
    for name in ("done", "new"):
        (out / "datasets" / name).mkdir(parents=True)
        (out / "traces" / name).mkdir(parents=True)
    (out / "test/done").mkdir(parents=True)
    (out / "test/done/test-report.json").write_text("{}")
    builds, calls = [], []
    monkeypatch.setattr(script.trace_datasets, "build", lambda name, *args: builds.append(name))

    def run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[3] == "test-eval":
            d = Path(cmd[-1])
            d.mkdir(parents=True)
            (d / "test-report.json").write_text("{}")

    monkeypatch.setattr(script.subprocess, "run", run)
    script.run(argparse.Namespace(only=None, downloads=tmp_path, meta=tmp_path / "meta", keep_datasets=keep))
    assert builds == ["new"]
    assert [cmd[3] for cmd in calls] == ["test-eval", "test-eval-report"]
    assert len(calls[-1][4:]) == 2
    for name in ("done", "new"):
        assert (out / "datasets" / name).exists() == keep
        assert (out / "traces" / name).is_dir()


def test_batch_failure_preserves_dataset(tmp_path, monkeypatch):
    script = load_script(monkeypatch)
    monkeypatch.setattr(script, "ROOT", tmp_path)
    dataset_path = tmp_path / "build/ml-traces/datasets/new"
    dataset_path.mkdir(parents=True)
    monkeypatch.setattr(script.trace_datasets, "build", lambda *args: None)

    def fail(cmd, **kwargs):
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(script.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        script.run(argparse.Namespace(only="new", downloads=tmp_path, meta=tmp_path / "meta", keep_datasets=False))
    assert dataset_path.is_dir()


def test_full_grid_reproduces_train_selection(tmp_path):
    source = dataset(tmp_path, test_reads=7)
    commands = {}
    for name in ("train", "test-eval"):
        out = tmp_path / name
        subprocess.run([sys.executable, "-m", "sppr_ml", name, "--data", str(source), "--out", str(out)],
                       check=True, capture_output=True, text=True)
        filename = "report.json" if name == "train" else "test-report.json"
        commands[name] = json.loads((out / filename).read_text())
    trained, tested = commands["train"], commands["test-eval"]
    assert trained["best"] == tested["best"]
    assert len(tested["trials"]) == len(evaluate.grid())
    for a, b in zip(trained["trials"], tested["trials"]):
        for key in ("params", "rounds", "scores"):
            assert a[key] == b[key]
    assert trained["labels"]["validation"]["mean"] == 1.5
    assert tested["labels"]["test"]["mean"] == 3.5
