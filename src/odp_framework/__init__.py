"""ONYX Data Platform — CLEX → bronze → silver ingestion framework.

A single package (versioned wheel) contains:

* ``config`` / ``bundle``: run at deploy time (Python DABs) and generate,
  for each landing container, a job and the silver SDP pipeline.
* ``registry``: consolidates the CLEX manifests into the table registry.
* ``ingest``: bronze with Auto Loader (no SDP), one table per iteration.
* ``sdp``: the silver SCD1 consolidation (AUTO CDC) inside the pipeline.
* ``tasks``: the entry points of the wheel tasks (register, ingest, report).

The runtime only depends on what Databricks Runtime already ships (pyspark,
databricks-sdk). PyYAML is only used at deploy time.
"""

__version__ = "0.4.0"

# Version of the configuration contract embedded in the pipeline. If the shape
# of the JSON changes, bump this number and the runtime rejects old configurations.
RUNTIME_CONTRACT = 3
