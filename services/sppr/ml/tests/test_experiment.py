from copy import deepcopy

import pytest

from sppr_ml.experiment import paired, markdown, ROUNDS


def outcome(seed, mean=10, p95=20, violations=0):
    return {"seed": seed, "latency": {"mean_ms": mean, "p95_ms": p95, "reads": 100, "errors": 0},
            "violations": violations}


@pytest.mark.parametrize("means,expected", [([9, 9, 9], .1), ([10, 10, 10], 0), ([11, 11, 11], -.1)])
def test_bootstrap_known_data(means, expected):
    got = paired([outcome(i, m) for i, m in enumerate(means)], [outcome(i) for i in range(3)], "age")
    assert ROUNDS == 10_000
    assert got.reduction == pytest.approx(expected)
    assert got.low == pytest.approx(expected)
    assert got.high == pytest.approx(expected)
    assert got.passed == (expected >= .05)


def test_paired_relative_not_ratio_of_means():
    a = [outcome(1, 90), outcome(0, 5)]
    b = [outcome(0, 10), outcome(1, 100)]
    got = paired(a, b, "age")
    assert got.reductions == pytest.approx([.5, .1])
    assert got.reduction == pytest.approx(.3)
    assert got == paired(a, b, "age")


@pytest.mark.parametrize("change", ["p95", "violations", "errors", "one_seed"])
def test_criterion(change):
    ml, base = [outcome(i, 8) for i in range(2)], [outcome(i) for i in range(2)]
    if change == "p95":
        for r in ml:
            r["latency"]["p95_ms"] = 22
    if change == "violations":
        ml[0]["violations"] = 1
    if change == "errors":
        ml[0]["latency"]["errors"] = 1
    if change == "one_seed":
        ml, base = ml[:1], base[:1]
    assert not paired(ml, base, "age").passed


@pytest.mark.parametrize("ml,base", [([], []), ([outcome(0)], [outcome(1)]),
                                      ([outcome(0), outcome(0)], [outcome(0)]),
                                      ([outcome(0, 0)], [outcome(0)]),
                                      ([outcome(0)], [outcome(0, float("nan"))])])
def test_bad_pairs(ml, base):
    with pytest.raises(ValueError):
        paired(ml, base, "age")


def test_markdown():
    p = {k: {"mean_ms": 10, "p95_ms": 20, "hot_read_share": .5, "copy_bytes": 1048576,
             "migrations": 1, "forecast_mae": 2, "plan_seconds": .1}
         for k in ("age", "recency", "frequency", "ewma", "lgbm")}
    report = {"cells": [{"scenario": "stable", "budget_bytes": 67108864, "policies": p,
                          "comparisons": [], "passed": False}]}
    text = markdown(report)
    assert "| stable | 64 |" in text
    assert "не выполнен" in text


def fixture_series(tmp_path):
    import json
    import hashlib
    from sppr_ml.experiment import BASELINES
    policies = [*BASELINES, "lgbm"]
    sim = {"policies": policies, "budgets_bytes": [67108864], "generator": {"scenario": "stable", "seed": 0}}
    manifest = {"finished": "2026-10-28T00:00:00Z", "git_sha": "fixture", "sha256": {},
                "config": {"scenarios": ["stable"], "seeds": [0, 1], "simulation": sim}}
    for seed in [0, 1]:
        pair = tmp_path / "pairs" / f"stable-{seed}"
        results = pair / "results"
        results.mkdir(parents=True)
        hashes = {}
        for policy in policies:
            r = outcome(seed, mean=8 if policy == "lgbm" else 10)
            config = deepcopy(sim)
            config["generator"]["seed"] = seed
            r.update(scenario="stable", budget_bytes=67108864, policy=policy, config=config,
                     versions={"git_sha": "fixture"}, hot_read_share=.5, copy_bytes=1048576,
                     migrations=1, forecast_mae=1, plan_seconds=.01)
            path = results / f"{policy}.json"
            path.write_text(json.dumps(r))
            hashes[f"results/{path.name}"] = hashlib.sha256(path.read_bytes()).hexdigest()
        path = pair / "pair.json"
        path.write_text(json.dumps(hashes))
        manifest["sha256"][f"stable-{seed}"] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return manifest


def test_processing_complete_matrix(tmp_path):
    from sppr_ml.experiment import process
    fixture_series(tmp_path)
    report = process(tmp_path)
    assert len(report["cells"]) == 1
    assert report["cells"][0]["passed"]
    assert len(report["cells"][0]["comparisons"]) == 4


def test_processing_rejects_corruption(tmp_path):
    from sppr_ml.experiment import process
    fixture_series(tmp_path)
    (tmp_path / "pairs/stable-0/results/age.json").write_text("{}")
    with pytest.raises(ValueError, match="result hash"):
        process(tmp_path)


def test_processing_rejects_incomplete_series(tmp_path):
    import json
    from sppr_ml.experiment import process
    manifest = fixture_series(tmp_path)
    manifest.pop("finished")
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="incomplete"):
        process(tmp_path)
