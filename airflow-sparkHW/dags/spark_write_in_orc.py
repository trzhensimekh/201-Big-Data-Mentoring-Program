from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_master = "spark://spark:7077"
csv_file = "/usr/local/spark/resources/data/movies.csv"
output_file = "/usr/local/spark/resources/data/output/movies.orc"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark_write_in_orc",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/test_airflow.py", # Spark application path created in airflow and spark cluster
    name="test_airflow",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[csv_file,output_file],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end