from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    
}

dag = DAG(
    'spark_sales_analysis1',
    default_args=default_args,
    description='A simple Spark job DAG',
    schedule_interval=timedelta(days=1),
)

spark_job = SparkSubmitOperator(
    task_id='spark_sales_analysis_task',
    application='/opt/airflow/spark/app/sales_analysis.py',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.submit.deployMode': 'client',
        'spark.driver.host': '{{ ti.hostname }}',
        'spark.driver.bindAddress': '0.0.0.0',
        'spark.network.timeout': '600s',
        'spark.executor.heartbeatInterval': '60s',
    },
    env_vars={
        'SPARK_INPUT_PATH': '/opt/airflow/spark/resources/sales.csv',
        'SPARK_OUTPUT_PATH': '/opt/airflow/spark/resources/sales_analysis_output'
    },
    dag=dag
)

#spark_job = BashOperator(
#    task_id='spark_sales_analysis_task',
#    bash_command='spark-submit --master spark://spark-master:7077 --name sales_analysis --verbose /opt/airflow/spark/app/sales_analysis.py',
#    dag=dag
#)





spark_job