import pymongo
from datetime import datetime
from stg.loaders.load_data_from_ext_system import LoadDataFromExtSystem
from lib.dict_util import to_dict


class LoadDataFromOrdersSystem(LoadDataFromExtSystem):
    def __init__(self, src_dburl, src_dbname, 
                 cert_path, dst_dburl,
                 workflow_key, last_val_key, 
                 default_value, filter_key_ext,
                 pkey_ext, dest_table_schema, 
                 src_collection_name, log, data_limit=50):

        super().__init__(dst_dburl=dst_dburl, workflow_key=workflow_key, 
                         last_val_key=last_val_key, default_value=default_value, 
                         dest_table_schema=dest_table_schema, data_limit=data_limit, 
                         filter_key_ext=filter_key_ext, pkey_ext=pkey_ext,
                         log=log, cert_path=cert_path)
        
        self._srcdburl = src_dburl
        self._srcdbname = src_dbname
        self._src_collection_name = src_collection_name

    def _get_last_val(self, data):
        return data[-1][self._last_val_key]

    def _get_data_from_ext_system(self):
        self._log.info(f"Getting data from {self._src_collection_name}")
        last_loaded_val = datetime.fromisoformat(self._cur_last_loaded_val)
        f = {self._filter_key_ext: {'$gt': last_loaded_val}}
        sort = [(self._filter_key_ext, 1)]

        with pymongo.MongoClient(self._srcdburl, tlsCAFile=self._cert_path) as client:
            db = client[self._srcdbname]
            data = list(db[self._src_collection_name].find(filter=f, sort=sort, 
                                                      limit=self._data_limit))
            return tuple(map(to_dict, data))
