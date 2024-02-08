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
def fill_dds_data_dag():
    @task_group(group_id="load_data_from_stg_to_dds")
    def load_data_from_stg_to_dds():
        tasks = []

        for n in ("dm_users", "dm_couriers", "dm_addresses", "dm_timestamps",
                  "dm_restaurants", "dm_products", "dm_orders", "fct_product_sales"):
            @task(task_id=f"fill_{n}")
            def t(name):
                exec_stored_proc(conn_id="PG_WAREHOUSE_CONNECTION", query=f"CALL dds.fill_{name}()")

            tasks.append(t(n))

        tasks[:-3] >> tasks[-3] >> tasks[-2] >> tasks[-1]
    
    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")

    t_start >> load_data_from_stg_to_dds() >> t_finish

fill_ddsdata_dag = fill_dds_data_dag()