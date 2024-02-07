import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'data_load', 'example'],
    is_paused_upon_creation=True
)
def load_bonus_data_dag():
    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")
    t_start >> t_finish

load_data_dag = load_bonus_data_dag()