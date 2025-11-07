# database.py
import pymongo
import redis

# --- Configuración de Conexión ---
MONGO_HOST = 'mongo'
MONGO_PORT = 27017
MONGO_DB_NAME = 'aseguradora_tp'

REDIS_HOST = 'redis'
REDIS_PORT = 6379
# ---------------------------------

def get_mongo_client():
    """Retorna una instancia del cliente de MongoDB."""
    try:
        client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        # Probar la conexión
        client.server_info() 
        print(f"Conexión a MongoDB ({MONGO_HOST}:{MONGO_PORT}) exitosa.")
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error al conectar a MongoDB: {e}")
        exit(1)

def get_mongo_db(client):
    """Retorna la base de datos específica del proyecto."""
    return client[MONGO_DB_NAME]

def get_redis_client():
    """Retorna una instancia del cliente de Redis."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        # Probar la conexión
        r.ping()
        print(f"Conexión a Redis ({REDIS_HOST}:{REDIS_PORT}) exitosa.")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Error al conectar a Redis: {e}")
        exit(1)

# --- Instancias Globales ---
# Estas instancias se crearán una vez y se importarán en otros módulos
try:
    mongo_client = get_mongo_client()
    db = get_mongo_db(mongo_client)
    r = get_redis_client()
    print("--- Conexiones listas ---")
except Exception as e:
    print(f"Error fatal durante la inicialización de las BBDD: {e}")
    exit(1)