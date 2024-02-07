from airflow.models.variable import Variable
from lib.db_connections import PGConnect, MongoConnect
from airflow.hooks.base import BaseHook


def get_mongo_conn():
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")
    port = Variable.get("MONGO_DB_PORT")

    return MongoConnect(host=host, port=port, dbuser=db_user, 
                        dbpassword=db_pw, replica_set=rs, auth_db=db, 
                        main_db=db).url(), cert_path, db

def get_pg_conn(conn_id):
    sslmode_key = "sslmode"
    conn = BaseHook.get_connection(conn_id)
    
    if sslmode_key in conn.extra_dejson:
        sslmode = conn.extra_dejson[sslmode_key]

    return PGConnect(host=conn.host, port=conn.port, dbname=conn.schema, 
                     dbuser=conn.login, dbpassword=conn.password, sslmode=sslmode).url()


def get_headers_base_url():
    return tuple(Variable.get(n) for n in ("DELIVERY_SYSTEM_HEADERS", "DELIVERY_SYSTEM_BASE_URL"))