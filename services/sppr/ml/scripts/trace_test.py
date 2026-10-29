"""Stage 11 trace TEST evaluations; reuse conversions, remove datasets on success.

Run from any directory with the ML venv Python. No downloads are performed.
Completed build/ml-traces/test/<name> directories are skipped on resume.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

import trace_datasets

ROOT = Path(__file__).resolve().parents[4]


def run(args) -> None:
    out = ROOT / "build/ml-traces"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT / "services/sppr/ml"), env.get("PYTHONPATH")]))
    cli = [sys.executable, "-m", "sppr_ml"]
    reports = []
    for name in [args.only] if args.only else trace_datasets.SOURCES:
        result = out / "test" / name
        dataset = out / "datasets" / name
        if not result.exists():
            print(f"== {name}", flush=True)
            trace_datasets.build(name, out, args.downloads, args.meta, ROOT / "build/bin")
            subprocess.run([*cli, "test-eval", "--data", str(dataset), "--out", str(result)],
                           cwd=ROOT, env=env, check=True)
        # Never remove data after a failed evaluation, or for an incomplete result.
        report = result / "test-report.json"
        if not report.is_file():
            raise ValueError(f"incomplete result directory (missing test-report.json): {result}")
        if not args.keep_datasets and dataset.exists():
            shutil.rmtree(dataset)
        reports.append(str(result))
    subprocess.run([*cli, "test-eval-report", *reports], cwd=ROOT, env=env, check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--downloads", type=Path, required=True)
    ap.add_argument("--meta", type=Path, required=True)
    ap.add_argument("--only", choices=sorted(trace_datasets.SOURCES))
    ap.add_argument("--keep-datasets", action="store_true")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
