import logging
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from lib.utils import get_mongo_conn, get_pg_conn
from stg.loaders.load_data_from_orders_system import LoadDataFromOrdersSystem


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def load_order_system_data_dag():
    update_ts_key = "update_ts"
    srcdb_url, cert_path, src_dbname = get_mongo_conn()
    dstdb_url = get_pg_conn("PG_WAREHOUSE_CONNECTION")
    dest_table_schema, pkey_ext = "stg", "_id"
    last_loaded_ts_default_val = "2022-01-01 00:00:00"

    @task_group(group_id="load_data_from_order_system_to_stg")
    def load_data_to_stg():
        tasks = []

        for n in (("orders", "users", "restaurants")):
            @task(task_id=f"load_{n}_data_from_order_system")
            def f(name):
                l = LoadDataFromOrdersSystem(src_dburl=srcdb_url, src_dbname=src_dbname, 
                                             dst_dburl=dstdb_url, workflow_key=f"ordersystem_{name}", 
                                             last_val_key=update_ts_key, 
                                             default_value=last_loaded_ts_default_val, 
                                             filter_key_ext=update_ts_key,
                                             src_collection_name=name, 
                                             dest_table_schema=dest_table_schema, 
                                             pkey_ext=pkey_ext, cert_path=cert_path, log=log)
            
                l.load_data()
     
            tasks.append(f(n))
        
        tasks

    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")

    t_start >> load_data_to_stg() >> t_finish

load_ordersystem_data_dag = load_order_system_data_dag()