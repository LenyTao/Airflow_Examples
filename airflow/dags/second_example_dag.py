# Импортируем необходимые библиотеки
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

# -------------------------------------

# Любые переменные среды
os.environ['CONFIG_PATH'] = "/airflow_examples"
# -------------------------------------

# Любые переменные python
DAG_ID = 'Second_Example_Dag'
# -------------------------------------

# Стандартные настройки для DAG могут задаваться так
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}
# -------------------------------------

# Для того чтобы airflow заметил наш DAG необходима эта сущность
# Стандартные настройки для DAG могут задаваться так тоже
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    tags=['SECOND_EXAMPLE'],
    schedule_interval="20 16 * * *",
    catchup=False
)
# -------------------------------------

hello_task_id = 'Hello_Task'
ls_task_id = 'Ls_Task'
cat_task_id = 'Cat_Task'
spark_shower_task_id = 'Spark_Shower_Task'

hello_task_name = f'{DAG_ID}.{hello_task_id}'
spark_shower_task_name = f'{DAG_ID}.{spark_shower_task_id}'

# Spark conf для spark operator в виде словаря
spark_conf_for_shower = {
    "spark.submit.deployMode": "client"
}
# -------------------------------------
main_path = "/airflow_examples"
core_path = f"{main_path}/second_example/SparkShower.jar"

# Выдёргиваем коннекшен из базы Airflow
connection = BaseHook.get_connection('Spark_Shower_Conn')
# -------------------------------------

# Прикрепим к нашему DAG таски, можно сделать на прямую или виде метода
with dag:
    def create_spark_submit_operator(task_id: str, task_name: str, spark_conf: dict):
        return SparkSubmitOperator(
            task_id=task_id,
            name=task_name,
            application_args=[
                f"USER=={connection.login}",
                f"PASSWORD==password='{connection.password}'"
            ],
            application=core_path,
            java_class="ru.Shower",
            conf=spark_conf,
            retries=0,
            dag=dag
        )


    hello_task = BashOperator(
        task_id=hello_task_id,
        bash_command='//$CONFIG_PATH/first_example/Airflow_Example_Script*',
        dag=dag)

    spark_shower_task = \
        create_spark_submit_operator(spark_shower_task_id, spark_shower_task_name, spark_conf_for_shower)

    ls_task = BashOperator(
        task_id=ls_task_id,
        bash_command='ls',
        dag=dag)

    cat_task = BashOperator(
        task_id=cat_task_id,
        bash_command='cat //$CONFIG_PATH/first_example/Airflow_Example_Script*',
        dag=dag)
# -------------------------------------


### Рисуем порядок как мы хотим:

## Последовательный порядок:

# [hello_task >> spark_shower_task]

##-------------------------------------

## Чередование

hello_task >> (ls_task, cat_task)
(ls_task, cat_task) >> spark_shower_task

##-------------------------------------


## Параллельные потоки

# [hello_task >> ls_task]
# [cat_task >> spark_shower_task]

##-------------------------------------



## Не важен порядок

##-------------------------------------
