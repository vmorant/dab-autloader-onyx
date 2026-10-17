# Fuente SDP — consolidación silver (SCD1, réplica de origen) de un contenedor.
#
# Mismas entidades que bronze (registro odp_entities); lee las tablas bronze que
# la tarea anterior del job acaba de actualizar. Los runs rechazados por
# register (odp_rejected_runs) no se aplican.

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
