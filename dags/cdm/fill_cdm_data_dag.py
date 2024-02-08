import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from lib.utils import exec_stored_proc

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def fill_cdm_data_dag():
    @task_group(group_id="load_data_from_dds_to_cdm")
    def load_data_from_dds_to_cdm():
        tasks = []

        for n in ("dm_settlement_report", "dm_courier_ledger"):
            @task(task_id=f"fill_{n}")
            def t(name):
                exec_stored_proc(conn_id="PG_WAREHOUSE_CONNECTION", query=f"CALL cdm.fill_{name}()")

            tasks.append(t(n))

        tasks
    
    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")

    t_start >> load_data_from_dds_to_cdm() >> t_finish

fill_ddsdata_dag = fill_cdm_data_dag()