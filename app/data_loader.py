#!/usr/bin/env python3

"""
Script de carga de datos (ETL) desde CSVs a MongoDB y Redis.

Este script lee los 5 CSVs de seguros (clientes, agentes, vehiculos, 
polizas, siniestros) desde una ruta proporcionada, transforma los datos 
al modelo de NoSQL diseñado y los carga en las colecciones de MongoDB 
y las claves de Redis correspondientes.

Uso:
    python data_loader.py /ruta/a/la/carpeta/con/csvs/
"""

import sys
import os
import csv
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from redis import Redis
from database import db, r
from utils import * 

CSV_FILES = {
    "clientes": "clientes.csv",
    "agentes": "agentes.csv",
    "vehiculos": "vehiculos.csv",
    "polizas": "polizas.csv",
    "siniestros": "siniestros.csv",
}

def open_csv_reader(path, filename):
    """Abre un archivo CSV y devuelve un DictReader."""
    file_path = os.path.join(path, filename)
    if not os.path.exists(file_path):
        print(f"Error: No se encontró el archivo {file_path}")
        sys.exit(1)
    return csv.DictReader(open(file_path, 'r', encoding='utf-8-sig'))


def main(base_path):

    print("Limpiando colecciones y Redis...")
    db.clientes.delete_many({})
    db.agentes.delete_many({})
    db.polizas.delete_many({})
    db.siniestros.delete_many({})
    r.flushdb()

    print("Cargando CSVs en memoria para referencias...")
    
    clientes_lookup = {
        parse_int(c['id_cliente']): c for c in open_csv_reader(base_path, CSV_FILES['clientes'])
    }
    
    agentes_lookup = {
        parse_int(a['id_agente']): a for a in open_csv_reader(base_path, CSV_FILES['agentes'])
    }
    
    vehiculos_lookup = {}
    for v in open_csv_reader(base_path, CSV_FILES['vehiculos']):
        id_cliente = parse_int(v['id_cliente'])
        if id_cliente not in vehiculos_lookup:
            vehiculos_lookup[id_cliente] = []
        vehiculos_lookup[id_cliente].append(v)
    
    print(f"Se encontraron {len(clientes_lookup)} clientes, {len(agentes_lookup)} agentes y datos de vehículos.")

    vehiculos_bulk_ops = []

    for v_list in vehiculos_lookup.values():
        for v in v_list:
            vehiculos_bulk_ops.append({
                "_id": parse_int(v['id_vehiculo']),
                "idVehiculo": parse_int(v['id_vehiculo']),
                "idCliente": parse_int(v['id_cliente']),
                "marca": v['marca'],
                "modelo": v['modelo'],
                "anio": parse_int(v['anio']),
                "patente": v['patente'],
                "nroChasis": v['nro_chasis'],
                "asegurado": parse_bool(v.get('asegurado', 'True')),
            })

    if vehiculos_bulk_ops:
        db.vehiculos.insert_many(vehiculos_bulk_ops)

    print(f"Se insertaron {len(vehiculos_bulk_ops)} vehículos en la colección 'vehiculos'.")


    print("Cargando Agentes...")
    agentes_bulk_ops = []
    for agente_id, agente in agentes_lookup.items():
        doc = {
            "_id": agente_id,
            "nombre": agente['nombre'],
            "apellido": agente['apellido'],
            "matricula": agente['matricula'],
            "telefono": agente['telefono'],
            "email": agente['email'],
            "zona": agente['zona'],
            "activo": parse_bool(agente['activo']),
            "stats": {
                "polizas_activas": 0,
                "polizas_total": 0,
                "siniestros_abiertos": 0,
                "siniestros_total": 0
            }
        }
        agentes_bulk_ops.append(doc)
        if doc['activo']:
            r.sadd("active_agentes", agente_id)
            
    if agentes_bulk_ops:
        db.agentes.insert_many(agentes_bulk_ops)

    print("Cargando Clientes e incrustando Vehículos...")
    clientes_bulk_ops = []
    for cliente_id, cliente in clientes_lookup.items():
        doc = {
            "_id": cliente_id,
            "nombre": cliente['nombre'],
            "apellido": cliente['apellido'],
            "dni": cliente['dni'],
            "email": cliente['email'],
            "telefono": cliente['telefono'],
            "direccion": cliente['direccion'],
            "ciudad": cliente['ciudad'],
            "provincia": cliente['provincia'],
            "activo": parse_bool(cliente['activo']),
            "vehiculos": [],
            "polizas_resumen": []
        }
        
        if cliente_id in vehiculos_lookup:
            for v in vehiculos_lookup[cliente_id]:
                doc['vehiculos'].append({
                    "id_vehiculo": parse_int(v['id_vehiculo']),
                    "marca": v['marca'],
                    "modelo": v['modelo'],
                    "anio": parse_int(v['anio']),
                    "patente": v['patente'],
                    "nro_chasis": v['nro_chasis']
                })
        
        clientes_bulk_ops.append(doc)
        if doc['activo']:
            r.sadd("active_clientes", cliente_id)
    
    if clientes_bulk_ops:
        db.clientes.insert_many(clientes_bulk_ops)

    print("Cargando Pólizas y denormalizando...")
    polizas_bulk_ops = []
    polizas_lookup = {}
    
    for poliza in open_csv_reader(base_path, CSV_FILES['polizas']):
        nro_poliza = poliza['nro_poliza']
        id_cliente = parse_int(poliza['id_cliente'])
        id_agente = parse_int(poliza['id_agente'])
        
        doc = {
            "_id": nro_poliza,
            "fecha_inicio": parse_date(poliza['fecha_inicio']),
            "fecha_fin": parse_date(poliza['fecha_fin']),
            "tipo": poliza['tipo'],
            "prima_mensual": parse_float(poliza['prima_mensual']),
            "cobertura_total": parse_float(poliza['cobertura_total']),
            "estado": poliza['estado'],
            "id_cliente": id_cliente,
            "id_agente": id_agente,
            "id_vehiculo": None, 
            "cliente_info": {},
            "agente_info": {},
            "vehiculo_info": {}
        }
        
        if id_cliente in clientes_lookup:
            c = clientes_lookup[id_cliente]
            doc['cliente_info'] = {
                "nombre_completo": f"{c['nombre']} {c['apellido']}",
                "activo": parse_bool(c['activo'])
            }
        
        if id_agente in agentes_lookup:
            a = agentes_lookup[id_agente]
            doc['agente_info'] = {
                "nombre_completo": f"{a['nombre']} {a['apellido']}"
            }
        
        if doc['tipo'] == 'Auto' and id_cliente in vehiculos_lookup:
            v = vehiculos_lookup[id_cliente][0] 
            doc['id_vehiculo'] = parse_int(v['id_vehiculo'])
            doc['vehiculo_info'] = {
                "patente": v['patente'],
                "descripcion": f"{v['marca']} {v['modelo']} ({v['anio']})"
            }
            
        polizas_bulk_ops.append(doc)
        polizas_lookup[nro_poliza] = doc 

    if polizas_bulk_ops:
        db.polizas.insert_many(polizas_bulk_ops)

    print("Cargando Siniestros y denormalizando...")
    siniestros_bulk_ops = []
    
    for siniestro in open_csv_reader(base_path, CSV_FILES['siniestros']):
        nro_poliza = siniestro['nro_poliza']
        poliza = polizas_lookup.get(nro_poliza)
        
        doc = {
            "_id": parse_int(siniestro['id_siniestro']),
            "fecha": parse_date(siniestro['fecha']),
            "tipo": siniestro['tipo'],
            "monto_estimado": parse_float(siniestro['monto_estimado']),
            "descripcion": siniestro['descripcion'],
            "estado": siniestro['estado'],
            "nro_poliza": nro_poliza,
            "id_cliente": None,
            "id_agente": None,
            "cliente_afectado": {}
        }
        
        if poliza:
            doc['id_cliente'] = poliza['id_cliente']
            doc['id_agente'] = poliza['id_agente']
            if poliza['id_cliente'] in clientes_lookup:
                c = clientes_lookup[poliza['id_cliente']]
                doc['cliente_afectado'] = {
                    "nombre_completo": f"{c['nombre']} {c['apellido']}",
                    "telefono": c['telefono']
                }
        
        siniestros_bulk_ops.append(doc)
        
    if siniestros_bulk_ops:
        db.siniestros.insert_many(siniestros_bulk_ops)

    print("Calculando estadísticas y resúmenes (Agregaciones)...")

    pipeline_clientes = [
        {
            "$lookup": {
                "from": "siniestros",
                "localField": "_id",
                "foreignField": "nro_poliza",
                "as": "siniestros_data"
            }
        },
        {
            "$group": {
                "_id": "$id_cliente",
                "polizas_resumen": {
                    "$push": {
                        "nro_poliza": "$_id",
                        "tipo": "$tipo",
                        "fecha_fin": "$fecha_fin",
                        "estado": "$estado",
                        "cobertura_total": "$cobertura_total",
                        "cant_siniestros": { "$size": "$siniestros_data" }
                    }
                },
                "stats_cobertura_total_activa": {
                    "$sum": { "$cond": [{ "$eq": ["$estado", "Activa"] }, "$cobertura_total", 0] }
                },
                "stats_polizas_activas": {
                    "$sum": { "$cond": [{ "$eq": ["$estado", "Activa"] }, 1, 0] }
                }
            }
        },
        {
            "$merge": {
                "into": "clientes",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ]
    db.polizas.aggregate(pipeline_clientes)

    pipeline_agentes_polizas = [
        {
            "$group": {
                "_id": "$id_agente",
                "polizas_activas": {
                    "$sum": { "$cond": [{ "$eq": ["$estado", "Activa"] }, 1, 0] }
                },
                "polizas_total": { "$count": {} }
            }
        },
        {
            "$merge": {
                "into": "agentes",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ]
    db.polizas.aggregate(pipeline_agentes_polizas)

    pipeline_agentes_siniestros = [
        {
            "$group": {
                "_id": "$id_agente",
                "siniestros_abiertos": {
                    "$sum": { "$cond": [{ "$eq": ["$estado", "Abierto"] }, 1, 0] }
                },
                "siniestros_total": { "$count": {} }
            }
        },
        {
            "$merge": {
                "into": "agentes",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ]
    db.siniestros.aggregate(pipeline_agentes_siniestros)

    print("Actualizando ranking de Redis (top_clientes_cobertura)...")
    pipeline = r.pipeline()
    for cliente in db.clientes.find({}, {"stats.cobertura_total_activa": 1}):
        cobertura = cliente.get("stats", {}).get("cobertura_total_activa", 0)
        if cobertura > 0:
            pipeline.zadd("top_clientes_cobertura", {cliente['_id']: cobertura})
    pipeline.execute()

    print("\n--- ¡Carga de datos completada exitosamente! ---")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Error: Debes proporcionar la ruta a la carpeta de los CSVs.")
        print("Uso: python loadData.py /ruta/a/los/csvs")
        sys.exit(1)
        
    csv_path = sys.argv[1]
    main(csv_path)