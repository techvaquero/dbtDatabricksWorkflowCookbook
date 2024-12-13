import os
from databricks.sdk import WorkspaceClient

#Add selection criteria here
selection_val = "+base +core"

#Set to debug
os.environ["DBT_DATABRICKS_CONNECTOR_LOG_LEVEL"] = "DEBUG"

w = WorkspaceClient()
t = w.tokens.create(lifetime_seconds=600)
os.environ['DBT_DATABRICKS_TOKEN'] = t.token_value
os.environ['DBT_ACCESS_TOKEN'] = t.token_value


if selection_val != "":
  selection_logic_str = f"-s {selection_val}"
  selection_logic = selection_logic_str.split(' ')
else:
  selection_logic_str = ""
  selection_logic = []

# import os
# import itertools
# from dbt.cli.main import dbtRunner, dbtRunnerResult

# dbt = dbtRunner()
# base_args = ["run", "--profiles-dir", os.getcwd()]
# cli_args = list(itertools.chain(base_args, selection_logic))

# res = dbt.invoke(cli_args)

# if res.success:
#   print("Success")
# else:
#   print(res)


import subprocess

path = subprocess.getoutput("which dbt")
profile_path = os.getcwd()

subprocess.call(f"""{path} run --profiles-dir {profile_path} --log-level DEBUG {selection_logic_str}""", shell=True)