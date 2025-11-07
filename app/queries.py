from database import db
from datetime import datetime, timedelta


def q1_clientes_activos_polizas_vigentes():
    pipeline = [
    {
        "$match": {
            "activo": True,
            "polizas_resumen.estado": "Activa"
        }
    },
    {
        "$project": {
            "_id": 0,
            "nombre": 1,
            "apellido": 1,
            "dni": 1,
            "email": 1,
            "telefono": 1,
            "direccion": 1,
            "ciudad": 1,
            "provincia": 1,
            "polizas": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$polizas_resumen",
                                "as": "poliza",
                                "cond": {"$eq": ["$$poliza.estado", "Activa"]}
                            }
                        },
                        "as": "poliza",
                        "in": {
                            "nro_poliza": "$$poliza.nro_poliza",
                            "tipo": "$$poliza.tipo",
                            "fecha_fin": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$$poliza.fecha_fin"
                                }
                            },
                            "estado": "$$poliza.estado",
                            "cobertura_total": "$$poliza.cobertura_total",
                            "cant_siniestros": "$$poliza.cant_siniestros"
                        }
                    }
            }
        }
    }
]
    return list(db.clientes.aggregate(pipeline))


def q2_siniestros_abiertos_cliente():
    pipeline = [
        {"$match": {"estado": "Abierto"}},
        {"$project": {
            "_id": 0,
            "Tipo": "$tipo",
            "Monto": "$monto_estimado",
            "cliente": "$cliente_afectado.nombre_completo"
        }}
    ]
    return list(db.siniestros.aggregate(pipeline))

def q3_vehiculos_con_cliente_poliza():
    pipeline = [
        {"$match": {"asegurado": True}},
        {"$addFields": {"idCliente_num": {"$toInt": "$idCliente"}}},
        {
            "$lookup": {
                "from": "clientes",
                "localField": "idCliente_num",
                "foreignField": "_id",
                "as": "cliente"
            }
        },
        {"$unwind": "$cliente"},
        {
            "$addFields": {
                "polizas_auto": {
                    "$filter": {
                        "input": "$cliente.polizas_resumen",
                        "as": "poliza",
                        "cond": {
                            "$eq": [{"$toLower": "$$poliza.tipo"}, "auto"]
                        }
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "Patente": "$patente",
                "Marca": "$marca",
                "Modelo": "$modelo",
                "Cliente": {"$concat": ["$cliente.nombre", " ", "$cliente.apellido"]},
                "Polizas_Auto": {
                    "$map": {
                        "input": "$polizas_auto",
                        "as": "p",
                        "in": {
                            "nro_poliza": "$$p.nro_poliza",
                            "tipo": "$$p.tipo",
                            "fecha_fin": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$$p.fecha_fin"
                                }
                            },
                            "estado": "$$p.estado",
                            "cobertura_total": "$$p.cobertura_total",
                            "cant_siniestros": "$$p.cant_siniestros"
                        }
                    }
                }
            }
        }
    ]

    return list(db.vehiculos.aggregate(pipeline))



def q4_clientes_sin_polizas_activas():
    return list(db.clientes.find({
        "polizas_resumen.estado": { "$nin": ["Activa"] }
    }, {
        "_id": 0,
        "nombre": {"$concat": ["$nombre", " ", "$apellido"]},
        "dni": 1,
        "email": 1,
        "telefono": 1,
        "direccion": 1,
        "ciudad": 1,
        "provincia": 1
    }))


def q5_agentes_activos_cant_polizas():
    pipeline = [
        {"$match": {"activo": True}},
        {"$project": {
            "_id": 0,
            "Agente": {"$concat": ["$nombre", " ", "$apellido"]},
            "Cantidad Polizas": "$polizas_total"
        }}
    ]
    return list(db.agentes.aggregate(pipeline))

def q6_polizas_vencidas_con_cliente():
    pipeline = [
        {"$match": {"estado": "Vencida"}},
        {
            "$project": {
                "_id": 0,
                "Poliza": "$_id",
                "Fecha de Inicio": {
                    "$dateToString": {
                        "format": "%d-%m-%Y", 
                        "date": "$fecha_inicio"
                    }
                },
                "Fecha de Vencimiento": {
                    "$dateToString": {
                        "format": "%d-%m-%Y",  
                        "date": "$fecha_fin"
                    }
                },
               
                "prima mensual": "$prima_mensual",
                "cobertura total": "$cobertura_total",
                "estado": "$estado",
                "Cliente": "$cliente_info.nombre_completo"
            }
        }
    ]
    return list(db.polizas.aggregate(pipeline))

def q7_top10_clientes_cobertura_total():
    pipeline = [
        {"$lookup": {
            "from": "polizas",
            "localField": "idCliente",
            "foreignField": "idCliente",
            "as": "polizas"
        }},
        {"$unwind": "$polizas"},
        {"$group": {
            "_id": {"idCliente": "$idCliente", "Cliente": {"$concat": ["$nombre", " ", "$apellido"]}},
            "CoberturaTotal": {"$sum": "$polizas.coberturaTotal"}
        }},
        {"$sort": {"CoberturaTotal": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "Cliente": "$_id.Cliente", "CoberturaTotal": 1}}
    ]
    return list(db.clientes.aggregate(pipeline))


def q8_siniestros_accidente_ultimo_anio():
    hace_un_anio = datetime.now() - timedelta(days=365)
    return list(db.siniestros.find(
        {"tipo": "Accidente", "fecha": {"$gte": hace_un_anio}},
        {"_id": 0, "Siniestro": "$idSiniestro", "Fecha": "$fecha", "Monto": "$montoEstimado"}
    ))


def q9_polizas_activas_ordenadas():
    return list(db.polizas.find(
        {"estado": "Activa"},
        {"_id": 0, "Poliza": "$nroPoliza", "FechaInicio": "$fechaInicio", "FechaFin": "$fechaFin"}
    ).sort("fechaInicio", 1))


def q10_polizas_suspendidas_estado_cliente():
    pipeline = [
        {"$match": {"estado": "Suspendida"}},
        {"$lookup": {
            "from": "clientes",
            "localField": "idCliente",
            "foreignField": "idCliente",
            "as": "cliente"
        }},
        {"$unwind": "$cliente"},
        {"$project": {
            "_id": 0,
            "Poliza": "$nroPoliza",
            "Cliente": {"$concat": ["$cliente.nombre", " ", "$cliente.apellido"]},
            "ClienteActivo": "$cliente.activo"
        }}
    ]
    return list(db.polizas.aggregate(pipeline))

# 11
def q11_clientes_multiples_vehiculos():
    pipeline = [
        {"$group": {"_id": "$idCliente", "CantidadVehiculos": {"$sum": 1}}},
        {"$match": {"CantidadVehiculos": {"$gt": 1}}},
        {"$lookup": {
            "from": "clientes",
            "localField": "_id",
            "foreignField": "idCliente",
            "as": "cliente"
        }},
        {"$unwind": "$cliente"},
        {"$project": {
            "_id": 0,
            "Cliente": {"$concat": ["$cliente.nombre", " ", "$cliente.apellido"]},
            "CantidadVehiculos": 1
        }}
    ]
    return list(db.vehiculos.aggregate(pipeline))

# 12
def q12_agentes_cant_siniestros():
    pipeline = [
        {"$lookup": {
            "from": "polizas",
            "localField": "idAgente",
            "foreignField": "idAgente",
            "as": "polizas"
        }},
        {"$unwind": {"path": "$polizas", "preserveNullAndEmptyArrays": False}},
        {"$lookup": {
            "from": "siniestros",
            "localField": "polizas.nroPoliza",
            "foreignField": "nroPoliza",
            "as": "siniestros"
        }},
        {"$project": {
            "_id": 0,
            "Agente": {"$concat": ["$nombre", " ", "$apellido"]},
            "CantidadSiniestros": {"$size": "$siniestros"}
        }}
    ]
    return list(db.agentes.aggregate(pipeline))

# 13
def q13_abm_clientes_demo():
    nuevo = {
        "idCliente": "999",
        "nombre": "Demo",
        "apellido": "Cliente",
        "dni": "12345678",
        "email": "demo@cliente.com",
        "telefono": "11112222",
        "direccion": "Av. Siempre Viva 123",
        "ciudad": "Springfield",
        "provincia": "Buenos Aires",
        "activo": True
    }
    db.clientes.insert_one(nuevo)
    return [nuevo]

# 14
def q14_alta_siniestro_demo():
    nuevo = {
        "idSiniestro": "9999",
        "nroPoliza": "POL1001",
        "fecha": datetime.now(),
        "tipo": "Accidente",
        "montoEstimado": 150000.0,
        "descripcion": "Choque demo",
        "estado": "Abierto"
    }
    db.siniestros.insert_one(nuevo)
    return [nuevo]

# 15
def q15_emision_poliza_demo():
    cliente = db.clientes.find_one({"idCliente": "1"})
    agente = db.agentes.find_one({"idAgente": "101"})
    if not cliente or not agente:
        return [{"Error": "Cliente o agente no válido"}]

    nueva = {
        "nroPoliza": "POL9999",
        "idCliente": cliente["idCliente"],
        "tipo": "Hogar",
        "fechaInicio": datetime.now(),
        "fechaFin": datetime.now() + timedelta(days=365),
        "primaMensual": 30000,
        "coberturaTotal": 1000000,
        "idAgente": agente["idAgente"],
        "estado": "Activa"
    }
    db.polizas.insert_one(nueva)
    return [nueva]
