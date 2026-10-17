# SDP source — silver consolidation (SCD1, replica of the source) of one container.
#
# Same entities as bronze (odp_entities registry); reads the bronze tables that
# the previous job task has just updated. Runs rejected by register
# (odp_rejected_runs) are not applied.

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from odp_framework.registry import load_active_entities, load_rejected_runs
from odp_framework.runtime import load_runtime_config
from odp_framework.sdp.silver import register_silver

spark = SparkSession.getActiveSession()
cfg = load_runtime_config(spark.conf.get("odp.config", None))
register_silver(
    spark,
    dp,
    cfg.with_entities(load_active_entities(spark, cfg)),
    rejected=load_rejected_runs(spark, cfg),
)
