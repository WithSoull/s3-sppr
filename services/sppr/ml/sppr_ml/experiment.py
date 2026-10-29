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
    reduction: float | None
    low: float | None
    high: float | None
    p95_change: float | None
    worst_p95_change: float | None
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
    for s in seeds:
        if "offered" in a[s] or "offered" in b[s]:
            validate_demand(a[s], b[s])
    if any(r["latency"]["reads"] == 0 for r in [*a.values(), *b.values()]) and "offered" in a[seeds[0]]:
        return Comparison(name, seeds, [], None, None, None, None, None, False, False)
    if not np.isfinite(av).all() or not np.isfinite(bv).all() or (av <= 0).any() or (bv <= 0).any():
        raise ValueError("latencies must be finite and positive")
    for s in seeds:
        if "offered" in a[s] or "offered" in b[s]:
            validate_demand(a[s], b[s])
        elif a[s]["latency"]["reads"] != b[s]["latency"]["reads"] or a[s]["latency"]["reads"] <= 0:
            raise ValueError("policies must complete the same nonempty demand")
    reductions = (bv[:, 0] - av[:, 0]) / bv[:, 0]
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(seeds), size=(ROUNDS, len(seeds)))
    low, high = np.percentile(reductions[draws].mean(axis=1), [2.5, 97.5])
    p95 = (av[:, 1] - bv[:, 1]) / bv[:, 1]
    # A stand run with history gaps had unusable features around them.
    ok = all(r.get("violations") == 0 and r["latency"].get("errors") == 0 and not r.get("history_gaps")
             for r in [*a.values(), *b.values()])
    passed = len(seeds) >= 2 and low >= .05 - 1e-12 and p95.mean() <= .05 + 1e-12 and ok
    return Comparison(name, seeds, reductions.tolist(), float(reductions.mean()), float(low), float(high),
                      float(p95.mean()), float(p95.max()), ok, bool(passed))



def verify_checkpoint(root, manifest, scenario, seed, *, all_files):
    pair_name = f"{scenario}-{seed}"
    directory = root / "pairs" / pair_name
    raw = (directory / "pair.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest["sha256"].get(pair_name):
        raise ValueError(f"pair checkpoint hash differs: {pair_name}")
    checkpoint = json.loads(raw)
    if all_files:
        actual = {str(p.relative_to(directory)) for p in directory.rglob("*")
                  if p.is_file() and p.name != "pair.json"}
        if actual != set(checkpoint):
            raise ValueError("checkpoint artifact set differs")
    for name, digest in checkpoint.items():
        path = Path(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("invalid checkpoint path")
        if not all_files and not name.startswith("results/"):
            continue
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"result hash differs; artifact hash differs: {name}")
    return checkpoint

def process(root: str | Path) -> dict:
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if not manifest.get("finished"):
        raise ValueError("series is incomplete; resume it before reporting")
    cfg = manifest["config"]
    sim = cfg["simulation"]
    stand = cfg.get("mode") == "stand"
    overhead = stand and cfg["stand"]["overhead"]
    if overhead:
        return process_overhead(root, manifest)
    # The stand series may compare ML with a subset of baselines; it then reports
    # the observed comparisons only and cannot claim the five-policy criterion.
    baselines = [p for p in BASELINES if p in sim["policies"]]
    if not stand and set(sim["policies"]) != {*BASELINES, "lgbm"}:
        raise ValueError("the criterion requires all five policies")
    if "lgbm" not in sim["policies"] or not baselines:
        raise ValueError("a comparison needs lgbm and at least one baseline")
    expected_seeds = set(cfg["seeds"])
    if len(expected_seeds) != len(cfg["seeds"]):
        raise ValueError("duplicate configured seeds")
    for scenario in cfg["scenarios"]:
        for seed in cfg["seeds"]:
            verify_checkpoint(root, manifest, scenario, seed, all_files=stand)
    groups = {}
    for path in sorted((root / "pairs").glob("*/results/*.json")):
        r = json.loads(path.read_text(encoding="utf-8"))
        key = (r["scenario"], r["budget_bytes"], r["policy"])
        if (key[0] not in cfg["scenarios"] or key[1] not in sim["budgets_bytes"]
                or key[2] not in sim["policies"] or r["seed"] not in expected_seeds):
            raise ValueError(f"unexpected result {path}")
        expected_config = deepcopy(sim)
        if stand:
            expected_config["generator"] = deepcopy(cfg["stand"]["load"]["generator"])
        expected_config["generator"]["scenario"] = key[0]
        expected_config["generator"]["seed"] = r["seed"]
        if r["config"] != expected_config:
            raise ValueError(f"result configuration differs from manifest: {path}")
        if stand:
            validate_stand(r, cfg["stand"], expected_config)
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
            comparisons = [paired(ml, rows(p), p) for p in baselines]
            policies = {}
            for p in sim["policies"]:
                r = rows(p)
                policies[p] = {k: float(np.mean([x["latency"][k] for x in r]))
                               for k in ("mean_ms", "p95_ms")}
                for key in ("hot_read_share", "copy_bytes", "migrations", "forecast_mae", "plan_seconds"):
                    values = [x.get(key) for x in r]
                    policies[p][key] = float(np.mean(values)) if all(v is not None for v in values) else None
            cells.append({"scenario": scenario, "budget_bytes": budget, "policies": policies,
                          "comparisons": [asdict(c) for c in comparisons],
                          "passed": len(baselines) == len(BASELINES) and all(c.passed for c in comparisons)})
    return {"mode": "stand" if stand else "sim", "bootstrap_rounds": ROUNDS, "bootstrap_seed": 0, "unit": "paired seed",
            "p95_aggregation": "mean of paired relative changes", "git_sha": manifest["git_sha"],
            "cells": cells}


def markdown(report: dict) -> str:
    lines = ["| Сценарий | Бюджет, МиБ | Возраст, мс | Давность, мс | Частота, мс | EWMA, мс | ML, мс | Критерий |",
             "|---|---:|---:|---:|---:|---:|---:|---|"]
    for cell in report["cells"]:
        # A stand series may run only some baselines; absent policies are н/д.
        values = " | ".join(f"{cell['policies'][p]['mean_ms']:.4f}" if p in cell["policies"] else "н/д"
                            for p in (*BASELINES, "lgbm"))
        lines.append(f"| {cell['scenario']} | {cell['budget_bytes'] / 2**20:g} | {values} | "
                     f"{'выполнен' if cell['passed'] else 'не выполнен'} |")
    lines += ["", "| Сценарий | Бюджет, МиБ | ML против | Уменьшение среднего | 95 % ДИ | Изменение p95 | Критерий пары |",
              "|---|---:|---|---:|---|---:|---|"]
    for cell in report["cells"]:
        for c in cell["comparisons"]:
            lines.append(f"| {cell['scenario']} | {cell['budget_bytes'] / 2**20:g} | {c['baseline']} | "
                         f"{format_optional(c['reduction'], '.2%')} | {format_optional(c['low'], '.2%')}…{format_optional(c['high'], '.2%')} | {format_optional(c['p95_change'], '+.2%')} | "
                         f"{'выполнен' if c['passed'] else 'не выполнен'} |")
    lines += ["", "| Сценарий | Бюджет, МиБ | Политика | p95, мс | Доля HOT | Перенесено, МиБ | Переносы | MAE прогноза | План, с |",
              "|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for cell in report["cells"]:
        for p, v in cell["policies"].items():
            lines.append(f"| {cell['scenario']} | {cell['budget_bytes'] / 2**20:g} | {p} | {v['p95_ms']:.4f} | "
                         f"{format_optional(v['hot_read_share'], '.2%')} | {format_optional(v['copy_bytes'], '.1f', 2**20)} | {format_optional(v['migrations'], '.1f')} | "
                         f"{format_optional(v['forecast_mae'], '.4f')} | {format_optional(v['plan_seconds'], '.3f')} |")
    lines += ["", "Средние по seed; ДИ — 10 000 парных bootstrap-повторов по seed. "
              "Изменение p95 — среднее парных относительных изменений."]
    if report.get("mode") == "stand":
        lines += ["", "Стенд: н/д означает, что показатель не наблюдался. Снимки API не доказывают все "
                  "промежуточные инварианты ёмкости/бюджета; без отдельной проверки сохранности "
                  "полный критерий остаётся неподтверждённым."]
    return "\n".join(lines) + "\n"



def format_optional(value, spec, scale=1):
    return "н/д" if value is None else format(value / scale, spec)


def validate_demand(a, b):
    if a["offered"] <= 0 or a["offered"] != b["offered"] or a["schedule_sha256"] != b["schedule_sha256"]:
        raise ValueError("offered demand/schedule differs between policies")
    for r in (a, b):
        errors = r["errors_by_kind"]
        if any(not isinstance(n, int) or n < 0 for n in errors.values()):
            raise ValueError("invalid error accounting")
        if (r["completed"] + sum(errors.values()) != r["offered"] or
                r["latency"]["reads"] != r["completed"] or r["latency"]["errors"] != sum(errors.values())):
            raise ValueError("unaccounted offered requests")


def validate_stand(r, config, expected):
    load = deepcopy(config["load"])
    load["generator"] = expected["generator"]
    if r["load_config"] != load or r["stand_config"] != config:
        raise ValueError("stand configuration differs from manifest")
    validate_demand(r, r)
    if not r.get("invariants_verified") and r.get("violations") == 0:
        raise ValueError("stand cannot claim verified invariants from snapshots alone")


def process_overhead(root, manifest):
    cfg = manifest["config"]
    modes = ("no-extension", "sppr-no-training", "sppr-update")
    groups = {p: [] for p in modes}
    budget = cfg["simulation"]["budgets_bytes"][0]
    if len(cfg["scenarios"]) != 1:
        raise ValueError("overhead requires one scenario")
    scenario = cfg["scenarios"][0]
    if not cfg["seeds"] or len(set(cfg["seeds"])) != len(cfg["seeds"]):
        raise ValueError("duplicate/empty overhead repetitions")
    for seed in cfg["seeds"]:
        pair_name = f"{scenario}-{seed}"
        directory = root / "pairs" / pair_name
        checkpoint = verify_checkpoint(root, manifest, scenario, seed, all_files=True)
        if {p.name for p in (directory / "results").glob("*.json")} != {f"{p}-{budget}.json" for p in modes}:
            raise ValueError("unexpected overhead result matrix")
        for policy in modes:
            path = directory / "results" / f"{policy}-{budget}.json"
            if f"results/{path.name}" not in checkpoint:
                raise ValueError("unhashed overhead result")
            r = json.loads(path.read_text())
            if (r["seed"] != seed or r["policy"] != policy or r["scenario"] != scenario or
                    r["budget_bytes"] != budget or r["versions"]["git_sha"] != manifest["git_sha"]):
                raise ValueError("overhead result identity differs")
            expected = deepcopy(cfg["simulation"])
            expected["generator"] = deepcopy(cfg["stand"]["load"]["generator"])
            expected["generator"].update(scenario=scenario, seed=seed)
            if r["config"] != expected:
                raise ValueError("overhead configuration differs")
            validate_stand(r, cfg["stand"], expected)
            run_dir = directory / f"{policy}-{budget}"
            if policy != "no-extension":
                samples = [json.loads(line) for line in (run_dir / "stats.jsonl").read_text().splitlines()]
                if not samples:
                    raise ValueError("missing docker stats")
            if policy == "sppr-update":
                training = [json.loads(line) for line in (run_dir / "training.jsonl").read_text().splitlines()]
                r["training_observed"] = any(t["success"] for t in training)
            groups[policy].append(r)
    base = {r["seed"]: r for r in groups["no-extension"]}
    cells = []
    for policy in modes[1:]:
        means, p95s = [], []
        ok = True
        for r in groups[policy]:
            b = base[r["seed"]]
            validate_demand(r, b)
            for value in (r, b):
                if not np.isfinite([value["latency"]["mean_ms"], value["latency"]["p95_ms"]]).all():
                    raise ValueError("latencies must be finite")
                ok &= not value["errors_by_kind"] and not value["warmup_errors_by_kind"]
            ok &= policy != "sppr-update" or r["training_observed"]
            if min(r["latency"]["mean_ms"], b["latency"]["mean_ms"],
                   r["latency"]["p95_ms"], b["latency"]["p95_ms"]) <= 0:
                means.append(None)
                p95s.append(None)
                ok = False
            else:
                means.append(r["latency"]["mean_ms"] / b["latency"]["mean_ms"] - 1)
                p95s.append(r["latency"]["p95_ms"] / b["latency"]["p95_ms"] - 1)
        mean = float(np.mean(means)) if all(x is not None for x in means) else None
        p95 = float(np.mean(p95s)) if all(x is not None for x in p95s) else None
        cells.append({"policy": policy, "mean_growth": mean, "p95_growth": p95,
                      "per_repetition_mean_growth": means, "per_repetition_p95_growth": p95s,
                      "errors_and_training_ok": bool(ok),
                      "passed": bool(ok and len(means) >= 2 and mean <= .05 + 1e-12 and p95 <= .05 + 1e-12)})
    return {"mode": "overhead", "git_sha": manifest["git_sha"], "repetitions": len(base),
            "warmup_seconds": cfg["stand"]["load"]["warmup_seconds"],
            "duration_seconds": cfg["stand"]["load"]["duration_seconds"],
            "aggregation": "mean of paired relative growth by repetition", "cells": cells,
            "passed": all(c["passed"] for c in cells)}


def overhead_markdown(report):
    lines = ["| Режим относительно no-extension | Рост среднего | Рост p95 | ≤ 5 % |",
             "|---|---:|---:|---|"]
    for c in report["cells"]:
        lines.append(f"| {c['policy']} | {format_optional(c['mean_growth'], '+.2%')} | {format_optional(c['p95_growth'], '+.2%')} | "
                     f"{'выполнен' if c['passed'] else 'не выполнен'} |")
    lines += ["", f"Повторов: {report['repetitions']}; прогрев: {report['warmup_seconds']:g} с; "
              f"измерение: {report['duration_seconds']:g} с. Среднее парных относительных изменений. "
              "Размещение фиксировано в COLD; миграции на паузе, планы не подтверждаются. "
              "sppr-update измеряет обучение и экспорт под общей квотой СППР; горячая активация не проверяется."]
    return "\n".join(lines) + "\n"

def command(args) -> None:
    report = process(args.results)
    text = overhead_markdown(report) if report.get("mode") == "overhead" else markdown(report)
    root = Path(args.results)
    stem = "overhead-report" if report.get("mode") == "overhead" else "experiment-report"
    (root / f"{stem}.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (root / f"{stem}.md").write_text(text, encoding="utf-8")
    print(text, end="")
