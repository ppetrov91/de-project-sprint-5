import logging
import pendulum
import json
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from lib.utils import get_headers_base_url, get_pg_conn
from stg.loaders.load_data_from_delivery_system import LoadDataFromDeliverySystem
from stg.loaders.load_data_from_delivery_system import LoadCouriersDataFromDeliverySystem


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def load_delivery_system_data_dag():
    dst_dburl = get_pg_conn("PG_WAREHOUSE_CONNECTION")
    dest_table_schema = "stg"
    last_loaded_ts_default_val = "2022-01-01 00:00:00"

    headers, base_url = get_headers_base_url()
    headers = json.loads(headers)

    @task_group(group_id="load_data_from_delivery_system_to_stg")
    def load_delivery_data_to_stg():
        
        @task
        def load_deliveries_data_from_order_system():
            l = LoadDataFromDeliverySystem(dst_dburl=dst_dburl, src_url=f"{base_url}/deliveries", 
                                           headers=headers, workflow_key="deliverysystem_deliveries", 
                                           last_val_key="delivery_ts", 
                                           default_value=last_loaded_ts_default_val, 
                                           pkey_ext="delivery_id", filter_key_ext="from", 
                                           sort_field="date", log=log,
                                           dest_table_schema=dest_table_schema)
            l.load_data()
     
        @task
        def load_couriers_data_from_order_system():
            pkey_ext = "_id"
            l = LoadCouriersDataFromDeliverySystem(dst_dburl=dst_dburl, src_url=f"{base_url}/couriers", 
                                                   headers=headers, 
                                                   workflow_key="deliverysystem_couriers", 
                                                   last_val_key="offset", default_value="0", 
                                                   pkey_ext=pkey_ext, filter_key_ext="offset", 
                                                   sort_field=pkey_ext, log=log,
                                                   dest_table_schema=dest_table_schema)
            l.load_data()

        [load_deliveries_data_from_order_system(), load_couriers_data_from_order_system()]

    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")

    t_start >> load_delivery_data_to_stg() >> t_finish

load_deliverysystem_data_dag = load_delivery_system_data_dag()