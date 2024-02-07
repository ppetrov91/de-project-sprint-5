from abc import ABC, abstractmethod
import psycopg
from psycopg.types.json import Jsonb
from psycopg.sql import SQL, Identifier
from datetime import datetime


class LoadDataFromExtSystem(ABC):
    def __init__(self, dst_dburl,
                 workflow_key, last_val_key, filter_key_ext, pkey_ext,
                 default_value, log, dest_table_schema="", cert_path="",
                 data_limit=50):
        
        self._dst_dburl = dst_dburl
        self._pkey_ext = pkey_ext
        self._workflow_key = workflow_key
        self._filter_key_ext = filter_key_ext
        self._last_val_key = last_val_key
        self._default_value = default_value
        self._dest_table_schema = dest_table_schema
        self._data_limit = data_limit
        self._cert_path = cert_path
        self._cur_last_loaded_val = ""
        self._log = log

    def _update_srv_settings(self, conn, last_val):
        update_settings_sql = """
        INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
        VALUES (%s, %s)
            ON CONFLICT (workflow_key)
            DO UPDATE
                  SET workflow_settings = EXCLUDED.workflow_settings
        """

        with conn.cursor() as cur:
            cur.execute(update_settings_sql, (self._workflow_key, 
                                              Jsonb({f"last_{self._last_val_key}": 
                                                     str(last_val)})))
    
    def _get_last_loaded_value(self):
        self._log.info(f"Getting last_{self._last_val_key} from {self._workflow_key}")

        read_setting_sql = """
        SELECT COALESCE(s.value, v.value) AS value
          FROM (SELECT %s AS value) v
          LEFT JOIN (SELECT s.workflow_settings->>%s AS value
                       FROM stg.srv_wf_settings s
                      WHERE s.workflow_key = %s
                    ) s
            ON (1 = 1)
        """
        with psycopg.connect(self._dst_dburl, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(read_setting_sql, (self._default_value, 
                                               f"last_{self._last_val_key}", 
                                               self._workflow_key))
                self._cur_last_loaded_val = cur.fetchone()[0]

    def _write_data_to_db(self, conn, data):
        self._log.info(f"Writing data to {self._dest_table_schema}.{self._workflow_key}")
                       
        write_data_sql = """
        INSERT INTO {}.{} (object_id, object_value, update_ts) 
        VALUES (%s, %s, %s)
            ON CONFLICT (object_id)
            DO UPDATE
                  SET object_value = EXCLUDED.object_value
                    , update_ts = EXCLUDED.update_ts
                WHERE {}.object_value != EXCLUDED.object_value
        """

        with conn.cursor() as cur:
            sql = SQL(write_data_sql).format(Identifier(self._dest_table_schema), 
                                             Identifier(self._workflow_key),
                                             Identifier(self._workflow_key))

            data = tuple(map(lambda x: 
                             (x[self._pkey_ext], Jsonb(x), 
                              x.get(self._last_val_key, datetime.utcnow())), 
                             data))

            cur.executemany(sql, data)

    def _write_data(self, data):
        if not data:
            return

        last_val = self._get_last_val(data)

        with psycopg.connect(self._dst_dburl, autocommit=True) as conn:
            with conn.transaction():
                self._write_data_to_db(conn, data)
                self._update_srv_settings(conn, last_val)

    def load_data(self):
        self._get_last_loaded_value()
        data = self._get_data_from_ext_system()
        self._log.info(f"Found {len(data)} new records")
        self._write_data(data)

    @abstractmethod
    def _get_data_from_ext_system(self):
        pass

    @abstractmethod
    def _get_last_val(self, data):
        pass