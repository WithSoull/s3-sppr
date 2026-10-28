"""Command line: `python -m sppr_ml train|report|evaluate|evaluate-report|experiment`.

train  — load one dataset directory, tune on validation, compare with
         baselines, write the model directory and report.json.
report — print a Markdown table of report.json files.
evaluate/evaluate-report — stage 11 fixed-parameter TEST evaluation and seed table.
"""

from __future__ import annotations

import argparse
import json
import math
import resource
import sys
import time
from dataclasses import asdict
from pathlib import Path

from . import data, evaluate, export, model


def _clean(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    return value


def train(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    rows = data.load(args.data, train_every=args.train_every)
    source = rows.source
    train_rows, validation = rows.of("train"), rows.of("validation")
    del rows
    if args.fit_only:
        if not args.params:
            raise ValueError("--fit-only requires fixed --params")
        hurdle = model.fit(train_rows, json.loads(args.params))
        manifest = export.write(Path(args.out), hurdle, train_rows, validation, {}, [])
        seconds = time.perf_counter() - started
        (Path(args.out) / "report.json").write_text(json.dumps({
            "model_version": manifest["model_version"], "seconds": seconds,
            "train_rows": len(train_rows), "params": json.loads(args.params),
            "selection": "fixed before the series; no validation selection",
        }, indent=2) + "\n", encoding="utf-8")
        print(f"{source}: {manifest['model_version']} in {seconds:.3f} s")
        return
    candidates = evaluate.grid()[:1] if args.quick else None
    if args.params:
        candidates = [json.loads(args.params)]
    hurdle, trials = evaluate.tune(train_rows, validation, candidates, log=lambda m: print(m, file=sys.stderr))
    compared = evaluate.compare(hurdle, train_rows, validation)
    difference = evaluate.versus_best_baseline(hurdle, train_rows, validation, compared)
    never_read = evaluate.never_read_check(hurdle, train_rows, validation)
    scores = _clean(evaluate.as_dict(compared))
    if hurdle.regressor is None:
        # Evaluated, but not a model the planner can load: see model.py.
        Path(args.out).mkdir(parents=True, exist_ok=False)
        version = "not exported: no repeated read in train, N̂ = p"
    else:
        version = export.write(Path(args.out), hurdle, train_rows, validation, scores, trials)["model_version"]
    peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1 << 20 if sys.platform == "darwin" else 1 << 10)
    report = {
        "source": source,
        "model_version": version,
        "train_every": args.train_every,
        "labels": {"train": evaluate.label_summary(train_rows), "validation": evaluate.label_summary(validation)},
        "scores": scores,
        "versus_best_baseline": _clean(asdict(difference)),
        "never_read": _clean(asdict(never_read)),
        "best": {"params": hurdle.params, "rounds": hurdle.rounds},
        "trials": _clean([{"params": t.params, "rounds": t.rounds, "seconds": t.seconds, "scores": asdict(t.scores)}
                          for t in trials]),
        "seconds": time.perf_counter() - started,
        "peak_rss_mib": peak_mib,
    }
    with open(Path(args.out) / "report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, allow_nan=False)
        f.write("\n")
    print(f"{source}: {version} in {report['seconds']:.0f} s, peak {peak_mib:.0f} MiB")


def report(args: argparse.Namespace) -> None:
    if args.summary:
        def verdict(d):
            return "лучше" if d["low"] > 0 else ("хуже" if d["high"] < 0 else "различие не доказано")

        print("| Источник | Лучшая базовая | Cov_K модели − базовой | 95 % ДИ | Вывод "
              "| Чтения ещё не читанных | Без них: разность, 95 % ДИ |")
        print("|---|---|---|---|---|---|---|")
        for path in args.reports:
            with open(path, encoding="utf-8") as f:
                r = json.load(f)
            d = r["versus_best_baseline"]
            n = r.get("never_read")
            extra = "— | —" if n is None else (
                f"{n['read_share']:.1%} | {n['without']['coverage']:+.4f} "
                f"({n['without']['low']:+.4f}…{n['without']['high']:+.4f}), {verdict(n['without'])}")
            print(f"| {r['source']} | {d['baseline']} | {d['coverage']:+.4f} | {d['low']:+.4f}…{d['high']:+.4f} "
                  f"| {verdict(d)} | {extra} |")
        return
    def fmt(v, digits=3):
        return "—" if v is None else f"{v:.{digits}f}"

    print("| Источник | Оценка | Cov_K | 95 % ДИ | Оракул | PR-AUC | Brier | Откл. Пуассона |")
    print("|---|---|---|---|---|---|---|---|")
    for path in args.reports:
        with open(path, encoding="utf-8") as f:
            r = json.load(f)
        for name, s in r["scores"].items():
            ci = s["coverage_ci"]
            print(f"| {r['source']} | {name} | {fmt(s['coverage'])} | {fmt(ci[0])}–{fmt(ci[1])} | "
                  f"{fmt(s['oracle_coverage'])} | {fmt(s['pr_auc'])} | {fmt(s['brier'], 4)} | {fmt(s['poisson_deviance'])} |")


def main() -> None:
    ap = argparse.ArgumentParser(prog="sppr_ml")
    sub = ap.add_subparsers(required=True)
    t = sub.add_parser("train", help="tune on validation and export a model directory")
    t.add_argument("--data", required=True, help="dataset directory from cmd/dataset")
    t.add_argument("--out", required=True, help="new model directory")
    t.add_argument("--train-every", type=int, default=900, help="keep a train snapshot every N seconds (0: all)")
    t.add_argument("--quick", action="store_true", help="one grid point; for smoke checks only")
    t.add_argument("--params", help="JSON hyperparameters chosen earlier: retrain without the grid search")
    t.add_argument("--fit-only", action="store_true", help="fit/export fixed --params without validation comparison (simulator updates)")
    t.set_defaults(func=train)
    r = sub.add_parser("report", help="Markdown table of report.json files")
    r.add_argument("reports", nargs="+")
    r.add_argument("--summary", action="store_true", help="only the paired difference with the best baseline")
    r.set_defaults(func=report)
    from . import final_evaluation
    f = sub.add_parser("evaluate", help="stage 11: train-only fixed fit, final TEST metrics")
    f.add_argument("--data", required=True, help="dataset directory from cmd/dataset")
    f.add_argument("--params", required=True, help="fixed JSON parameters; no test tuning")
    f.add_argument("--out", required=True, help="new directory for evaluate.json")
    f.set_defaults(func=final_evaluation.command)
    fr = sub.add_parser("evaluate-report", help="mean and min–max TEST metrics over seed directories")
    fr.add_argument("dirs", nargs="+")
    fr.set_defaults(func=final_evaluation.report)
    from .experiment import command
    e = sub.add_parser("experiment", help="paired seed bootstrap and placement tables")
    e.add_argument("results", help="completed cmd/experiment series directory")
    e.set_defaults(func=command)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
