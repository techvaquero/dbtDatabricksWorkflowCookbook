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

# %pip install --upgrade databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import subprocess
path = subprocess.getoutput("which dbt") #.call(["which","dbt"])

# COMMAND ----------

print(path)

# COMMAND ----------

dbutils.widgets.text("catalog", defaultValue="main", label="Catalog")

dbutils.widgets.text("schema", defaultValue="dustinvannoy_dev", label="Schema")

dbutils.widgets.text("selection_logic", defaultValue="")

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
t = w.tokens.create(lifetime_seconds=300)
os.environ['DBT_DATABRICKS_TOKEN'] = t.token_value
os.environ['DBT_ACCESS_TOKEN'] = t.token_value
# print(os.environ['DBT_ACCESS_TOKEN'])

# COMMAND ----------

# Setup environment variables for profile. Each person could have their own single node cluster with these set or could modify profiles.yml directly in their repos folder.
import os
os.environ["DBT_DATABRICKS_CONNECTOR_LOG_LEVEL"] = "DEBUG"

os.environ['DBT_DATABRICKS_HOST'] = "https://e2-dogfood.staging.cloud.databricks.com"
os.environ['DBT_DATABRICKS_HTTP_PATH'] = "/sql/1.0/warehouses/dd43ee29fedd958d"
# os.environ['DBT_DATABRICKS_HTTP_PATH'] = dbutils.secrets.get("my_scope", "my_path")

os.environ['DBT_DATABRICKS_SCHEMA'] = dbutils.widgets.get("schema")
os.environ['DBT_DATABRICKS_CATALOG'] = dbutils.widgets.get("catalog")

print(spark.conf.get("spark.databricks.clusterUsageTags.clusterId"))


# COMMAND ----------

selection_val = dbutils.widgets.get("selection_logic")
if selection_val != "":
  selection_logic = f"--select {selection_val}"
else:
  selection_logic = ""

os.environ["dbtselect"] = selection_logic

# COMMAND ----------

# MAGIC %sh mkdir -p /local_disk0/tmp/.dbt ; echo $' 
# MAGIC databricks_target:
# MAGIC   target: local
# MAGIC   outputs:
# MAGIC     #    #run DBT locally from your IDE and execute on a SQL warehouse (https://docs.getdbt.com/reference/warehouse-setups/databricks-setup)
# MAGIC     #    #Make sure you have pip install dbt-databricks in your local env
# MAGIC     #    #Run the project locally with:
# MAGIC     #    #DBT_DATABRICKS_HOST=xxx.cloud.databricks.com  DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx DBT_DATABRICKS_TOKEN=dapixxxx dbt run
# MAGIC     local:
# MAGIC       type: databricks
# MAGIC       catalog: main
# MAGIC       schema: "{{ env_var(\'DBT_DATABRICKS_SCHEMA\') }}"
# MAGIC       host: "{{ env_var(\'DBT_DATABRICKS_HOST\') }}"
# MAGIC       http_path: "{{ env_var(\'DBT_DATABRICKS_HTTP_PATH\') }}"
# MAGIC       token: "{{ env_var(\'DBT_ACCESS_TOKEN\') }}"
# MAGIC       threads: 24' > /local_disk0/tmp/.dbt/profiles.yml

# COMMAND ----------

# from databricks.sdk.core import credentials_provider

# COMMAND ----------

# profile_path = '/local_disk0/tmp/.dbt'
profile_path = os.getcwd()

subprocess.call(f"""{path} run --profiles-dir {profile_path} --profile databricks_target --vars '{{"catalog":"main", "schema":"dustinvannoy_dev", "DATABRICKS_HOST":"https://e2-dogfood.staging.cloud.databricks.com", "warehouse_path":"/sql/1.0/warehouses/dd43ee29fedd958d"}}' {selection_logic}""", shell=True)
# --select base.raw_audit_logs
#  --target local --vars '{"catalog":"main", "schema":"dustinvannoy_dev", "DATABRICKS_HOST":"https://e2-dogfood.staging.cloud.databricks.com", "warehouse_path":"/sql/1.0/warehouses/669300f0124d764"}'

# COMMAND ----------

# %sh -e dbt run --profiles-dir /local_disk0/tmp/.dbt $dbtselect

# COMMAND ----------

# MAGIC %md
# MAGIC Alternative commands that could be useful. Feel free to remove.

# COMMAND ----------

# Alternative code
# import subprocess
# results = subprocess.run([f"dbt run {selection_logic}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
# print(results)


# COMMAND ----------

# import dbt
import os
from dbt.cli.main import dbtRunner, dbtRunnerResult

dbt = dbtRunner()
cli_args = ["run", "--profiles-dir", os.getcwd(), "--vars", '{"catalog":"main", "schema":"dustinvannoy_dev", "DATABRICKS_HOST":"https://e2-dogfood.staging.cloud.databricks.com", "warehouse_path":"/sql/1.0/warehouses/dd43ee29fedd958d"}']
res = dbt.invoke(cli_args)

# COMMAND ----------

print(res)
