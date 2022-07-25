from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

Table_Name = Variable.get("Table_Name")

default_args = {
  'owner': 'airflow'
}

notebook_params = {
	"Table_Name" : Table_Name,
    "jdbcHostname" : "54.245.193.229",
    "jdbcDatabase" : "KAGGLE",
    "jdbcPort" : 1433
}

with DAG('create_table',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'create_table',
    databricks_conn_id = 'databricks_default',
	notebook_params=notebook_params,
    job_id = 597286771004993
  )
  