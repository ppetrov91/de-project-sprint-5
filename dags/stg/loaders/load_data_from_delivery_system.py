import urllib.parse
import requests
import json
from stg.loaders.load_data_from_ext_system import LoadDataFromExtSystem
from datetime import datetime


class LoadDataFromDeliverySystem(LoadDataFromExtSystem):
    def __init__(self, src_url, headers, 
                 dst_dburl, workflow_key, 
                 last_val_key, default_value, 
                 filter_key_ext, pkey_ext, 
                 sort_field, dest_table_schema, log,
                 sort_direction="asc", data_limit=50):
        
        super().__init__(dst_dburl=dst_dburl, workflow_key=workflow_key, 
                         last_val_key=last_val_key, default_value=default_value, 
                         dest_table_schema=dest_table_schema, data_limit=data_limit, 
                         filter_key_ext=filter_key_ext, pkey_ext=pkey_ext, log=log)

        self._src_url = src_url
        self._headers = headers
        self._sort_field = sort_field
        self._sort_direction = sort_direction

    def _get_last_val(self, data):
        last_val = str(max(map(lambda x: datetime.fromisoformat(x[self._last_val_key]), data)))
        return last_val[:last_val.find('.')]

    def _get_data_from_ext_system(self):
        params = {
            self._filter_key_ext: self._cur_last_loaded_val, 
            "sort_field": self._sort_field,
            "sort_direction": self._sort_direction,
            "limit": self._data_limit
        }

        t = urllib.parse.urlencode(params)
        response = requests.get(f"{self._src_url}?{t}", headers=self._headers)
        response.raise_for_status()
        return json.loads(response.content)
    
class LoadCouriersDataFromDeliverySystem(LoadDataFromDeliverySystem):
    def _get_last_val(self, data):
        return str(int(self._cur_last_loaded_val) + len(data))
