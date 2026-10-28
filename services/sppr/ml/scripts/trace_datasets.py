"""Build the stage 8 trace datasets: convert each local trace, then cut features.

Parameters are fixed here before any training (docs/ml-spec.md, «Трассы»): the
key share keeps train + validation near 5 M rows, the acceptance-profile size;
periods are 2/7, 1/7, 4/7 of the observed range; snapshots every 15 min.

    python scripts/trace_datasets.py --downloads DIR --meta FILE [--only NAME]

Needs build/bin/trace-convert and build/bin/dataset (make ml-sppr-traces builds
them). Existing output directories are kept, so an interrupted run resumes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

META_URL = "https://cache-datasets.s3.amazonaws.com/cache_dataset_txt/2023_metaCDN/"
COS_ORIGIN = "2020-01-01T00:00:00Z"
SNAPSHOT = "15m"

# name: (converter arguments with {downloads}/{meta} placeholders, key share)
SOURCES = {
    "meta-eag": (["-source", "meta", "-input", "{meta}", "-source-url", META_URL + "{meta_name}",
                  "-bucket", "meta-eag", "-source-share", "0.2", "-allow-inferred-success"], 0.004),
    "ibm-cos-000": (["-source", "ibm-cos", "-input", "{downloads}/IBMObjectStoreTrace000Part0.tgz",
                     "-bucket", "ibm-cos-000", "-origin", COS_ORIGIN, "-allow-inferred-success"], 0.1),
    "ibm-cos-001": (["-source", "ibm-cos", "-input", "{downloads}/IBMObjectStoreTrace001Part0.tgz",
                     "-bucket", "ibm-cos-001", "-origin", COS_ORIGIN, "-allow-inferred-success"], 0.025),
    "ibm-cos-002": (["-source", "ibm-cos", "-input", "{downloads}/IBMObjectStoreTrace002Part0.tgz",
                     "-bucket", "ibm-cos-002", "-origin", COS_ORIGIN, "-allow-inferred-success"], 1.0),
    "docker-syd01": (["-source", "docker", "-zone", "syd01", "-input", "{downloads}/DockerRegistryTraces.tar.gz"], 0.6),
}


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def periods(manifest: dict) -> tuple[str, str, str, str]:
    """2/7, 1/7, 4/7 of the observed range; the end is exclusive, after the last event."""
    start = _time(manifest["time_range"]["start"])
    end = _time(manifest["time_range"]["end"]) + timedelta(milliseconds=1)
    span = end - start
    return _iso(start), _iso(start + span * 2 / 7), _iso(start + span * 3 / 7), _iso(end)


def build(name: str, out: Path, downloads: Path, meta: Path, bin_dir: Path) -> None:
    args, share = SOURCES[name]
    events = out / "traces" / name
    if not events.exists():
        fill = {"downloads": str(downloads), "meta": str(meta), "meta_name": meta.name}
        subprocess.run([str(bin_dir / "trace-convert"), *[a.format(**fill) for a in args],
                        "-key-share", str(share), "-out", str(events)], check=True)
    dataset = out / "datasets" / name
    if dataset.exists():
        return
    start, train_end, validation_end, end = periods(json.loads((events / "manifest.json").read_text()))
    subprocess.run([str(bin_dir / "dataset"), "-catalog", str(events / "catalog.json"),
                    "-events", str(events / "events.jsonl"), "-start", start, "-end", end,
                    "-train-end", train_end, "-validation-end", validation_end, "-format", "csv.gz",
                    "-snapshot-every", SNAPSHOT, "-train-every", SNAPSHOT, "-out", str(dataset)], check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", type=Path, required=True, help="directory with the IBM archives")
    ap.add_argument("--meta", type=Path, required=True, help="local Meta CDN .csv.zst")
    ap.add_argument("--out", type=Path, default=Path("build/ml-traces"))
    ap.add_argument("--bin", type=Path, default=Path("build/bin"))
    ap.add_argument("--only", choices=sorted(SOURCES))
    args = ap.parse_args()
    for name in [args.only] if args.only else SOURCES:
        print(f"== {name}", flush=True)
        build(name, args.out, args.downloads, args.meta, args.bin)


if __name__ == "__main__":
    main()
