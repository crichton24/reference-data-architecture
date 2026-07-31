# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze maintenance
# MAGIC
# MAGIC COPY INTO writes one file per source file, which over time leaves bronze
# MAGIC with many medium-sized files and unclustered statistics. Weekly OPTIMIZE
# MAGIC compacts them; VACUUM reclaims storage from superseded versions.
# MAGIC
# MAGIC VACUUM's retention is deliberately left at the 7-day default — shortening
# MAGIC it breaks time travel and any in-flight readers.

# COMMAND ----------

TABLES = [
    "nyc_transit.bronze.yellow_trips",
    "nyc_transit.bronze.green_trips",
]

for table in TABLES:
    print(f"Optimizing {table}")
    spark.sql(f"OPTIMIZE {table} ZORDER BY (period)")
    spark.sql(f"VACUUM {table}")
    print(spark.sql(f"DESCRIBE DETAIL {table}").select("numFiles", "sizeInBytes").collect())
