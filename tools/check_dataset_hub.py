"""Opt-in live Hub reader check; prints schema/provenance, never dataset cells.

Example (install requirements-datasets.txt first)::

    python -B tools/check_dataset_hub.py --repository owner/dataset \
        --config default --split train --column text --rows 2

This reads a few public or already-authorized dataset rows. It does not call a
model, change the application configuration, or publish anything to the Hub.
Normal unit tests do not invoke this script or require network access.
"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_bindings import DatasetBindingManager, SourceBinding, TaskBinding
from dataset_sources import SourceSpec, open_source
from dataset_templates import render_template


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True, help="Hugging Face owner/dataset ID")
    parser.add_argument("--config", default=None)
    parser.add_argument("--split", required=True)
    parser.add_argument("--revision", default=None)
    parser.add_argument("--column", action="append", dest="columns", required=True)
    parser.add_argument("--rows", type=int, default=2)
    args = parser.parse_args(argv)
    if args.rows < 1:
        parser.error("--rows must be positive")
    if len(set(args.columns)) != len(args.columns):
        parser.error("--column names must be distinct")

    source = open_source(SourceSpec("live-check", "huggingface", args.repository,
                                    config=args.config, split=args.split, revision=args.revision))
    aliases = {column: "column_{}".format(index) for index, column in enumerate(args.columns)}
    try:
        schema = source.schema()
        print(json.dumps({
            "columns": schema.columns, "revision": schema.resolved_spec.revision,
            "config": schema.resolved_spec.config, "split": schema.resolved_spec.split,
        }, ensure_ascii=False), flush=True)
        with DatasetBindingManager("live-reader-check", [SourceBinding(source, aliases)]) as manager:
            for index in range(args.rows):
                job_id = "row-{}".format(index)
                bound = manager.bind(job_id)
                if manager.bind(job_id) is not bound:
                    raise RuntimeError("Repeated job binding allocated new values.")
                restored = TaskBinding.from_dict(json.loads(json.dumps(bound.to_dict())))
                if restored.to_dict() != bound.to_dict():
                    raise RuntimeError("Snapshot round trip changed the binding.")
                for alias, value in restored.values.items():
                    if render_template("{{ds:" + alias + "}}", restored.values) != value:
                        raise RuntimeError("Template rendering changed a bound value.")
                if bound.sources[0].row_id != str(index):
                    raise RuntimeError("Source row order differs from the requested sequence.")
            print(json.dumps({
                "status": "passed", "bound_jobs": len(manager.snapshot()),
                "mapped_columns": args.columns, "checks": [
                    "live rows", "same-job reuse", "snapshot round trip", "literal rendering"
                ],
            }), flush=True)
    except (ValueError, RuntimeError) as exc:
        print("Hub reader check failed: {}".format(exc), file=sys.stderr)
        return 1
    finally:
        source.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
