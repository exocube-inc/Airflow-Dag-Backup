from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

bronze_table_name = Variable.get("Table_Name")
bronze_schema_name = Variable.get("bronze_schema_name")
silver_table_name = Variable.get("Table_Name")
silver_schema_name = Variable.get("silver_schema_name")

default_args = {
  'owner': 'airflow'
}

notebook_params = {
    "Bronze_Table_Name": bronze_table_name,
    "Bronze_Schema_Name":bronze_schema_name,
	"Silver_Table_Name": silver_table_name,
    "Silver_Schema_Name":silver_schema_name,
	"Hostname" : "34.220.36.97",
    "Database" : "exoflow",
    "Port" : 3306,
    "jdbcHostname" : "54.245.193.229",
    "jdbcDatabase" : "KAGGLE",
    "jdbcPort" : 1433,
    "Table_Metadata":'Table_Metadata',
    "Process_Status_Table":'Process_Status_Table'
}

with DAG('source_to_bronze_to_silver',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:
  
  opr_run_now = DatabricksRunNowOperator(
    task_id = 'source_to_bronze',
    databricks_conn_id = 'databricks_default',
	notebook_params=notebook_params,
    job_id = 409176538952911
  )
  
   opr_submit_run = DatabricksSubmitRunOperator(
        task_id = 'bronze_to_silver',
		databricks_conn_id = 'databricks_default',
		notebook_params=notebook_params,
		job_id = 698189557074937
    )

opr_submit_run >> opr_run_now
  
  