import json
import queries
from cache_tags import QUERY_TAGS
from database import r

def query_dispatch(q_name):
    if(q_name.startswith("q13")):
        return dispatch13()
    
    if(q_name.startswith("q14")):
        return dispatch14()
    
    if(q_name.startswith("q15")):
        return dispatch15()

    cached = r.get(q_name)
    if cached:
        print(f"From Redis: {q_name}")
        return json.loads(cached)

    func = getattr(queries, q_name, None)
    if func is None:
        raise ValueError(f"No existe la función '{q_name}' en {queries.__name__}")
    
    result = func()
    r.set(q_name, json.dumps(result), ex=3600)
    print(f"From Mongo: {q_name}")
    return result


def dispatch13():
    invalidate_queries("clientes")
    return queries.q13_abm_clientes()

def dispatch14():
    invalidate_queries("siniestros")
    return queries.q14_alta_siniestro()

def dispatch15():
    invalidate_queries("polizas")
    return queries.q15_emision_nueva_poliza()


def invalidate_queries(tag):
    for q_name, tags in QUERY_TAGS.items():
        if tag in tags:
            r.delete(q_name)
