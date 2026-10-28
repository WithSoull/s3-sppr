"""Paired placement outcomes; resample seeds, never individual read latencies.

The reported reduction is the mean of per-seed relative reductions. A single
bootstrap draw uses the same seed indices in the model and baseline. p95 change
uses the same paired relative calculation, without an interval requirement.
"""
from __future__ import annotations

import json
import hashlib
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np

BASELINES = ("age", "recency", "frequency", "ewma")
ROUNDS = 10_000


@dataclass
class Comparison:
    baseline: str
    seeds: list[int]
    reductions: list[float]
    reduction: float
    low: float
    high: float
    p95_change: float
    worst_p95_change: float
    invariants_ok: bool
    passed: bool


def paired(ml: list[dict], baseline: list[dict], name: str, *, seed: int = 0) -> Comparison:
    def indexed(rows):
        out = {}
        for r in rows:
            if r["seed"] in out:
                raise ValueError("duplicate seed")
            out[r["seed"]] = r
        return out

    a, b = indexed(ml), indexed(baseline)
    if not a or a.keys() != b.keys():
        raise ValueError("paired seed sets must be nonempty and identical")
    seeds = sorted(a)
    av = np.array([[a[s]["latency"][k] for k in ("mean_ms", "p95_ms")] for s in seeds])
    bv = np.array([[b[s]["latency"][k] for k in ("mean_ms", "p95_ms")] for s in seeds])
    if not np.isfinite(av).all() or not np.isfinite(bv).all() or (av <= 0).any() or (bv <= 0).any():
        raise ValueError("latencies must be finite and positive")
    for s in seeds:
        if a[s]["latency"]["reads"] != b[s]["latency"]["reads"] or a[s]["latency"]["reads"] <= 0:
            raise ValueError("policies must complete the same nonempty demand")
    reductions = (bv[:, 0] - av[:, 0]) / bv[:, 0]
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(seeds), size=(ROUNDS, len(seeds)))
    low, high = np.percentile(reductions[draws].mean(axis=1), [2.5, 97.5])
    p95 = (av[:, 1] - bv[:, 1]) / bv[:, 1]
    ok = all(r.get("violations") == 0 and r["latency"].get("errors") == 0
             for r in [*a.values(), *b.values()])
    passed = len(seeds) >= 2 and low >= .05 - 1e-12 and p95.mean() <= .05 + 1e-12 and ok
    return Comparison(name, seeds, reductions.tolist(), float(reductions.mean()), float(low), float(high),
                      float(p95.mean()), float(p95.max()), ok, bool(passed))


def process(root: str | Path) -> dict:
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if not manifest.get("finished"):
        raise ValueError("series is incomplete; resume it before reporting")
    cfg = manifest["config"]
    sim = cfg["simulation"]
    if set(sim["policies"]) != {*BASELINES, "lgbm"}:
        raise ValueError("the criterion requires all five policies")
    expected_seeds = set(cfg["seeds"])
    if len(expected_seeds) != len(cfg["seeds"]):
        raise ValueError("duplicate configured seeds")
    for scenario in cfg["scenarios"]:
        for seed in cfg["seeds"]:
            pair_name = f"{scenario}-{seed}"
            directory = root / "pairs" / pair_name
            raw = (directory / "pair.json").read_bytes()
            if hashlib.sha256(raw).hexdigest() != manifest["sha256"].get(pair_name):
                raise ValueError(f"pair checkpoint hash differs: {pair_name}")
            checkpoint = json.loads(raw)
            for name, digest in checkpoint.items():
                if not name.startswith("results/"):
                    continue
                path = directory / name
                if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    raise ValueError(f"result hash differs: {name}")
    groups = {}
    for path in sorted((root / "pairs").glob("*/results/*.json")):
        r = json.loads(path.read_text(encoding="utf-8"))
        key = (r["scenario"], r["budget_bytes"], r["policy"])
        if (key[0] not in cfg["scenarios"] or key[1] not in sim["budgets_bytes"]
                or key[2] not in sim["policies"] or r["seed"] not in expected_seeds):
            raise ValueError(f"unexpected result {path}")
        expected_config = deepcopy(sim)
        expected_config["generator"]["scenario"] = key[0]
        expected_config["generator"]["seed"] = r["seed"]
        if r["config"] != expected_config:
            raise ValueError(f"result configuration differs from manifest: {path}")
        if r["versions"]["git_sha"] != manifest["git_sha"]:
            raise ValueError(f"revision differs from manifest: {path}")
        groups.setdefault(key, []).append(r)
    cells = []
    for scenario in cfg["scenarios"]:
        for budget in sim["budgets_bytes"]:
            def rows(policy):
                values = groups.get((scenario, budget, policy), [])
                if {v["seed"] for v in values} != expected_seeds or len(values) != len(expected_seeds):
                    raise ValueError(f"missing/duplicate pairs: {scenario}/{budget}/{policy}")
                return values

            ml = rows("lgbm")
            comparisons = [paired(ml, rows(p), p) for p in BASELINES]
            policies = {}
            for p in sim["policies"]:
                r = rows(p)
                policies[p] = {k: float(np.mean([x["latency"][k] for x in r]))
                               for k in ("mean_ms", "p95_ms")}
                for key in ("hot_read_share", "copy_bytes", "migrations", "forecast_mae", "plan_seconds"):
                    policies[p][key] = float(np.mean([x[key] for x in r]))
            cells.append({"scenario": scenario, "budget_bytes": budget, "policies": policies,
                          "comparisons": [asdict(c) for c in comparisons],
                          "passed": all(c.passed for c in comparisons)})
    return {"bootstrap_rounds": ROUNDS, "bootstrap_seed": 0, "unit": "paired seed",
            "p95_aggregation": "mean of paired relative changes", "git_sha": manifest["git_sha"],
            "cells": cells}


def markdown(report: dict) -> str:
    lines = ["| Сценарий | Бюджет, МиБ | Возраст, мс | Давность, мс | Частота, мс | EWMA, мс | ML, мс | Критерий |",
             "|---|---:|---:|---:|---:|---:|---:|---|"]
    for cell in report["cells"]:
        values = " | ".join(f"{cell['policies'][p]['mean_ms']:.4f}" for p in (*BASELINES, "lgbm"))
        lines.append(f"| {cell['scenario']} | {cell['budget_bytes'] / 2**20:g} | {values} | "
                     f"{'выполнен' if cell['passed'] else 'не выполнен'} |")
    lines += ["", "| Сценарий | Бюджет, МиБ | ML против | Уменьшение среднего | 95 % ДИ | Изменение p95 | Критерий пары |",
              "|---|---:|---|---:|---|---:|---|"]
    for cell in report["cells"]:
        for c in cell["comparisons"]:
            lines.append(f"| {cell['scenario']} | {cell['budget_bytes'] / 2**20:g} | {c['baseline']} | "
                         f"{c['reduction']:.2%} | {c['low']:.2%}…{c['high']:.2%} | {c['p95_change']:+.2%} | "
                         f"{'выполнен' if c['passed'] else 'не выполнен'} |")
    lines += ["", "| Сценарий | Бюджет, МиБ | Политика | p95, мс | Доля HOT | Перенесено, МиБ | Переносы | MAE прогноза | План, с |",
              "|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for cell in report["cells"]:
        for p, v in cell["policies"].items():
            lines.append(f"| {cell['scenario']} | {cell['budget_bytes'] / 2**20:g} | {p} | {v['p95_ms']:.4f} | "
                         f"{v['hot_read_share']:.2%} | {v['copy_bytes'] / 2**20:.1f} | {v['migrations']:.1f} | "
                         f"{v['forecast_mae']:.4f} | {v['plan_seconds']:.3f} |")
    lines += ["", "Средние по seed; ДИ — 10 000 парных bootstrap-повторов по seed. "
              "Изменение p95 — среднее парных относительных изменений."]
    return "\n".join(lines) + "\n"


def command(args) -> None:
    report = process(args.results)
    text = markdown(report)
    root = Path(args.results)
    (root / "experiment-report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (root / "experiment-report.md").write_text(text, encoding="utf-8")
    print(text, end="")
