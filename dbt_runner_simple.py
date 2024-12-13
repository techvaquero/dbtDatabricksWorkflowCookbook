# Databricks notebook source
# MAGIC %md
# MAGIC # DBT Runner
# MAGIC Use this notebook to run DBT Databricks models from within the same Databricks Repo. See the profiles.yml that is stored in the same repo as this notebook and relies on environment variables.
# MAGIC 1. Set Databricks Secrets for HOST, TOKEN, HTTP_PATH (warehouse). Modify command 5 to pull correct secrets for your testing.
# MAGIC 2. Choose Catalog and Schema for dbt to use when writing
# MAGIC 3. Selection Logic
# MAGIC   1. Leave empty to run all models. Otherwise provide a model name or other valid [dbt select clause](https://docs.getdbt.com/reference/node-selection/syntax#specifying-resources) which will be added after the --select parameter.  
# MAGIC   Examples:  
# MAGIC     `dbt_gold_nyctaxi_trip` to run only the model with that name  
# MAGIC     `tag:nyctaxi_sql` to run everything with this tag.
# MAGIC     
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install dbt-databricks==1.8.7 

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", defaultValue="main", label="Catalog")

dbutils.widgets.text("schema", defaultValue="dustinvannoy_dev", label="Schema")

dbutils.widgets.text("selection_logic", defaultValue="+base +core")

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
t = w.tokens.create(lifetime_seconds=600)
os.environ['DBT_DATABRICKS_TOKEN'] = t.token_value
os.environ['DBT_ACCESS_TOKEN'] = t.token_value

os.environ["DBT_DATABRICKS_CONNECTOR_LOG_LEVEL"] = "DEBUG"

# COMMAND ----------

selection_val = dbutils.widgets.get("selection_logic")
if selection_val != "":
  selection_logic_str = f"-s {selection_val}"
  selection_logic = selection_logic_str.split(' ')
else:
  selection_logic_str = ""
  selection_logic = []

# COMMAND ----------

import os
import itertools
from dbt.cli.main import dbtRunner, dbtRunnerResult

dbt = dbtRunner()
base_args = ["run", "--profiles-dir", os.getcwd(), "--log-level", "DEBUG"]
cli_args = list(itertools.chain(base_args, selection_logic))

res = dbt.invoke(cli_args)

# COMMAND ----------

if res.success:
  print("Success")
else:
  print(res)

# COMMAND ----------

# import subprocess

# path = subprocess.getoutput("which dbt")
# profile_path = os.getcwd()

# subprocess.call(f"""{path} run --profiles-dir {profile_path} --log-level DEBUG {selection_logic_str}""", shell=True)
