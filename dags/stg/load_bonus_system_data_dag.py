import os
import logging
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from lib.utils import get_pg_conn
from stg.loaders.load_data_from_bonus_system import LoadDataFromBonusSystem

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def load_bonus_system_data_dag():
    src_dburl = get_pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    dst_dburl = get_pg_conn("PG_WAREHOUSE_CONNECTION")

    @task_group(group_id="load_data_from_bonus_system_to_stg")
    def load_data_to_stg():
        tasks = []

        for n in ("events", "ranks", "users"):
            @task(task_id=f"load_{n}_data_from_bonus_system")
            def f(name):
                read_data_sql_filepath = os.path.join(os.path.dirname(__file__), 
                                                      f"loaders/sql/get_{name}_data.sql")
                write_data_sql_filepath = os.path.join(os.path.dirname(__file__), 
                                                       f"loaders/sql/save_{name}_data.sql")

                l = LoadDataFromBonusSystem(src_dburl=src_dburl, dst_dburl=dst_dburl, 
                                            workflow_key=f"bonussystem_{name}", last_val_key="id",
                                            default_value="-1",
                                            read_data_sql_filepath=read_data_sql_filepath,
                                            write_data_sql_filepath=write_data_sql_filepath,
                                            log=log)
                
                l.load_data()

            tasks.append(f(n))
        
        tasks
    
    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")
    t_start >> load_data_to_stg() >> t_finish

load_bonussystem_data_dag = load_bonus_system_data_dag()