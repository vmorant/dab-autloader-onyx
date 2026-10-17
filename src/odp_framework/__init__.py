"""ONYX Data Platform — framework de ingesta CLEX → bronze → silver.

Un mismo paquete (wheel versionada) contiene:

* ``config`` / ``bundle``: se ejecutan en el deploy (Python DABs) y generan,
  por cada contenedor del landing, un job y la pipeline SDP de silver.
* ``registry``: consolida los manifiestos de CLEX en el registro de tablas.
* ``ingest``: bronze con Auto Loader (sin SDP), una tabla por iteración.
* ``sdp``: la consolidación SCD1 de silver (AUTO CDC) dentro de la pipeline.
* ``tasks``: los entry points de las tareas wheel (register, ingest, report).

El runtime solo depende de lo que ya trae Databricks Runtime (pyspark,
databricks-sdk). PyYAML solo se usa en el deploy.
"""

__version__ = "0.4.0"

# Versión del contrato de configuración embebida en la pipeline. Si cambia la
# forma del JSON, se sube este número y el runtime rechaza configuraciones viejas.
RUNTIME_CONTRACT = 3
