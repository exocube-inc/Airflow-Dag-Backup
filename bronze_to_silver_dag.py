from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

silver_table_name = Variable.get("silver_table_name")
silver_schema_name = Variable.get("silver_schema_name")

default_args = {
  'owner': 'airflow'
}

notebook_params = {
	"Silver_Table_Name": silver_table_name,
    "Silver_Schema_Name":silver_schema_name,
	"Hostname" : "34.220.36.97",
    "Database" : "exoflow",
    "Port" : 3306,
    "Table_Metadata":'Table_Metadata',
    "Process_Status_Table":'Process_Status_Table'
}

with DAG('bronze_to_silver',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'bronze_to_silver',
    databricks_conn_id = 'databricks_default',
	notebook_params=notebook_params,
    job_id = 698189557074937
  )
  