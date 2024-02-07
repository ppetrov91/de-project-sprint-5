class PGConnect:
    def __init__(self, host, port, dbname, dbuser, dbpassword, sslmode="require"):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._dbuser = dbuser
        self._dbpassword = dbpassword
        self._sslmode = sslmode

    def url(self):
        return f"""host={self._host}
            port={self._port}
            dbname={self._dbname}
            user={self._dbuser}
            password={self._dbpassword}
            target_session_attrs=read-write 
            sslmode={self._sslmode}"""


class MongoConnect:
    def __init__(self, host, port, dbuser, dbpassword, 
                 replica_set, auth_db, main_db):
        self._host = host
        self._port = port
        self._dbuser = dbuser
        self._dbpassword = dbpassword
        self._replica_set = replica_set
        self._auth_db = auth_db
        self._main_db = main_db

    def url(self):
        return f"mongodb://{self._dbuser}:{self._dbpassword}@{self._host}:{self._port}/?replicaSet={self._replica_set}&authSource={self._auth_db}"