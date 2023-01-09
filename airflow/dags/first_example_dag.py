# Импортируем необходимые библиотеки
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# -------------------------------------

# Любые переменные среды
os.environ['CONFIG_PATH'] = "/airflow_examples"
# -------------------------------------

# Любые переменные python
DAG_ID = 'First_Example_Dag'
TASK_FIRST_EXAMPLE_ID = 'Example_Run'
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
    tags=['FIRST_EXAMPLE'],
    schedule_interval="15 15 * * *",
    catchup=False
)
# -------------------------------------

first_task_name = f'{DAG_ID}.{TASK_FIRST_EXAMPLE_ID}'

# Прикрепим к нашему DAG первую таску в Bash Operator
with dag:
    first_example_task = BashOperator(
        task_id=first_task_name,
        bash_command='//$CONFIG_PATH/first_example/Airflow_Example_Script*',
        dag=dag)
# -------------------------------------
