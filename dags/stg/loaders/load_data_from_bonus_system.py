import psycopg
from stg.loaders.load_data_from_ext_system import LoadDataFromExtSystem
from pathlib import Path


class LoadDataFromBonusSystem(LoadDataFromExtSystem):
    def __init__(self, src_dburl, dst_dburl,
                 workflow_key, last_val_key, 
                 default_value, read_data_sql_filepath, write_data_sql_filepath, log,
                 filter_key_ext="", pkey_ext="", data_limit=50):

        super().__init__(dst_dburl=dst_dburl, workflow_key=workflow_key,
                         last_val_key=last_val_key, default_value=default_value,
                         filter_key_ext=filter_key_ext, pkey_ext=pkey_ext, 
                         log=log, data_limit=data_limit)
        
        self._src_dburl = src_dburl
        self._read_data_sql_filepath = read_data_sql_filepath
        self._write_data_sql_filepath = write_data_sql_filepath

    def _get_data_from_ext_system(self):
        with psycopg.connect(self._src_dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                query = Path(self._read_data_sql_filepath).read_text()
                cur.execute(query, (self._cur_last_loaded_val, self._data_limit))
                return cur.fetchall()

    def _get_last_val(self, data):
        return data[-1][0]
    
    def _write_data_to_db(self, conn, data):
        with conn.cursor() as cur:
            query = Path(self._write_data_sql_filepath).read_text()
            cur.executemany(query, data)