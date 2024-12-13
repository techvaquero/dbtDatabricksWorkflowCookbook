# Databricks notebook source
import os

os.environ["DBT_RUN_BACKFILL"] = "false"
os.environ["DBT_CUSTOMER"] = "dustin.vannoy"

# COMMAND ----------

print(os.environ["DBT_CUSTOMER"])

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE main.dustinvannoy_dev_tmp.raw_clusters
