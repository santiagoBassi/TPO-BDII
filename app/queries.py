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
        return list(db.vehiculos.aggregate( [
        {"$match": {"asegurado": True}},
        {"$addFields": {"idCliente_num": {"$toInt": "$idCliente"}}},
        {
            "$lookup": {
                "from": "clientes",
                "localField": "idCliente",
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
    ]))





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
            "localField": "_id",
            "foreignField": "id_cliente",
            "as": "polizas"
        }},
        {"$unwind": "$polizas"},
        {"$group": {
            "_id": {"idCliente": "$_id", "Cliente": {"$concat": ["$nombre", " ", "$apellido"]}},
            "CoberturaTotal": {"$sum": "$polizas.cobertura_total"}
        }},
        {"$sort": {"CoberturaTotal": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "Cliente": "$_id.Cliente", "CoberturaTotal": 1}}
    ]
    return list(db.clientes.aggregate(pipeline))


def q8_siniestros_accidente_ultimo_anio():
    hace_un_anio = datetime.now() - timedelta(days=365)
    pipeline = [
        {
            "$match": {
                "tipo": "Accidente",
                "fecha": {"$gte": hace_un_anio}
            }
        },
        {
            "$project": {
                "_id": 0,
                "Siniestro": "$_id",
                "Fecha": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$fecha"
                    }
                },
                "Monto": "$monto_estimado",
                "Descripcion": "$descripcion",
                "Estado": "$estado"
            }
        }
    ]
    return list(db.siniestros.aggregate(pipeline))


def q9_polizas_activas_ordenadas():
    pipeline = [
        {"$match": {"estado": "Activa"}},
        {"$sort": {"fecha_inicio": 1}},
        {"$project": {
            "_id": 0,
            "Poliza": "$_id",
            "FechaInicio": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$fecha_inicio"
                }
            },
            "FechaFin": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$fecha_fin"
                }
            }
        }}
    ]
    return list(db.polizas.aggregate(pipeline))


def q10_polizas_suspendidas_estado_cliente():
    pipeline = [
        {"$match": {"estado": "Suspendida"}},
        {"$lookup": {
            "from": "clientes",
            "localField": "id_cliente",
            "foreignField": "_id",
            "as": "cliente"
        }},
        {"$unwind": "$cliente"},
        {"$project": {
            "_id": 0,
            "Poliza": "$_id",
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
            "foreignField": "_id",
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
            "localField": "_id",
            "foreignField": "id_agente",
            "as": "polizas"
        }},
        {"$unwind": {"path": "$polizas", "preserveNullAndEmptyArrays": False}},
        {"$lookup": {
            "from": "siniestros",
            "localField": "polizas._id",
            "foreignField": "nro_poliza",
            "as": "siniestros"
        }},
        {"$addFields": {
            "cantidad_siniestros": {"$size": "$siniestros"}
        }},
        {"$group": {
            "_id": {
                "idAgente": "$_id",
                "nombre": "$nombre",
                "apellido": "$apellido"
            },
            "CantidadSiniestros": {"$sum": "$cantidad_siniestros"}
        }},
        {"$project": {
            "_id": 0,
            "Agente": {"$concat": ["$_id.nombre", " ", "$_id.apellido"]},
            "CantidadSiniestros": 1
        }}
    ]
    return list(db.agentes.aggregate(pipeline))
def q13_abm_clientes():
    opcion = input("Ingrese opción (A=Alta, B=Baja, M=Modificación): ").strip().upper()

    if opcion == "A":
        print("\n--- Alta de nuevo cliente ---")
        nuevo = {
            "nombre": input("Nombre: ").strip(),
            "apellido": input("Apellido: ").strip(),
            "dni": input("DNI: ").strip(),
            "email": input("Email: ").strip(),
            "telefono": input("Teléfono: ").strip(),
            "direccion": input("Dirección: ").strip(),
            "ciudad": input("Ciudad: ").strip(),
            "provincia": input("Provincia: ").strip(),
            "activo": True,
            "vehiculos": [],
            "polizas_resumen": []
        }

        # Calcular nuevo ID
        ultimo = db.clientes.find_one(sort=[("_id", -1)])
        nuevo["_id"] = (ultimo["_id"] + 1) if ultimo else 1

        db.clientes.insert_one(nuevo)
        print(f"\nCliente {nuevo['nombre']} {nuevo['apellido']} agregado con ID {nuevo['_id']}.")
        return [nuevo]

    elif opcion == "B":
        print("\n--- Baja lógica de cliente ---")
        dni = input("Ingrese DNI del cliente a dar de baja: ").strip()
        cliente = db.clientes.find_one({"dni": dni})

        if not cliente:
            print("No se encontró ningún cliente con ese DNI.")
            return []

        if not cliente.get("activo", True):
            print("El cliente ya estaba inactivo.")
            return []

        db.clientes.update_one({"_id": cliente["_id"]}, {"$set": {"activo": False}})
        print(f"Cliente {cliente['nombre']} {cliente['apellido']} marcado como inactivo.")
        return [cliente]

    elif opcion == "M":
        print("\n--- Modificación de cliente ---")
        dni = input("Ingrese DNI del cliente a modificar: ").strip()
        cliente = db.clientes.find_one({"dni": dni})

        if not cliente:
            print("No se encontró ningún cliente con ese DNI.")
            return []

        print(f"Editando cliente: {cliente['nombre']} {cliente['apellido']}")
        campos_modificables = ["email", "telefono", "direccion", "ciudad", "provincia", "activo"]

        updates = {}
        for campo in campos_modificables:
            valor_actual = cliente.get(campo, "")
            nuevo_valor = input(f"{campo.capitalize()} [{valor_actual}]: ").strip()
            if nuevo_valor != "":
                if campo == "activo":
                    updates[campo] = nuevo_valor.lower() in ("true", "1", "si", "sí")
                else:
                    updates[campo] = nuevo_valor

        if updates:
            db.clientes.update_one({"_id": cliente["_id"]}, {"$set": updates})
            print("Cliente actualizado correctamente.")
        else:
            print("No se realizaron cambios.")
        return [db.clientes.find_one({"dni": dni})]

    else:
        print("Opción no válida.")
        return []

def q14_alta_siniestro():
    print("\n--- Alta de nuevo siniestro ---")

    nro_poliza = input("Ingrese número de póliza asociada (ej: POL1001): ").strip()
    poliza = db.polizas.find_one({"_id": nro_poliza})

    if not poliza:
        print("No existe una póliza con ese número.")
        return []

    if poliza.get("estado", "").lower() != "activa":
        print(f"La póliza {nro_poliza} no está activa (estado: {poliza.get('estado')}).")
        return []

    id_cliente = poliza.get("id_cliente")
    id_agente = poliza.get("id_agente")
    cliente = db.clientes.find_one({"_id": id_cliente})

    if not cliente:
        print(f"No se encontró el cliente asociado a la póliza {nro_poliza}.")
        return []

    tipo = input("Tipo de siniestro (Accidente, Robo, Incendio, etc.): ").strip() or "Accidente"
    descripcion = input("Descripción breve: ").strip() or "Sin descripción"
    monto_str = input("Monto estimado: ").strip()

    try:
        monto_estimado = float(monto_str) if monto_str else 0.0
    except ValueError:
        print("Monto inválido. Se usará 0.0")
        monto_estimado = 0.0

    estado = "Abierto"

    ultimo = db.siniestros.find_one(sort=[("_id", -1)])
    nuevo_id = (ultimo["_id"] + 1) if ultimo else 1

    nuevo = {
        "_id": nuevo_id,
        "fecha": datetime.now(),
        "tipo": tipo,
        "monto_estimado": monto_estimado,
        "descripcion": descripcion,
        "estado": estado,
        "nro_poliza": nro_poliza,
        "id_cliente": id_cliente,
        "id_agente": id_agente,
        "cliente_afectado": {
            "nombre_completo": f"{cliente['nombre']} {cliente['apellido']}",
            "telefono": cliente.get("telefono", "")
        }
    }

    db.siniestros.insert_one(nuevo)

    print(f"\nSiniestro creado correctamente con ID {nuevo_id} para la póliza {nro_poliza}.")
    return [nuevo]



def q15_emision_nueva_poliza():
    print("\n--- Emisión de nueva póliza ---")

    dni_cliente = input("Ingrese DNI del cliente: ").strip()
    cliente = db.clientes.find_one({"dni": dni_cliente})

    if not cliente:
        print("No existe un cliente con ese DNI.")
        return []
    
    if not cliente.get("activo", True):
        print(f"El cliente {cliente['nombre']} {cliente['apellido']} no está activo.")
        return []

    id_cliente = cliente["_id"]

    id_agente = input("Ingrese el ID del agente: ").strip()
    agente = db.agentes.find({"_id": id_agente})

    if not agente:
        print("No existe un agente con esa ID.")
        return []

    if not agente.get("activo", True):
        print(f"El agente {agente['nombre']} no está activo.")
        return []

    id_agente = agente["_id"]

    usa_vehiculo = input("¿La póliza está asociada a un vehículo? (s/n): ").strip().lower()
    vehiculo_info = {}
    id_vehiculo = None

    if usa_vehiculo == "s":
        patente = input("Ingrese la patente del vehículo: ").strip().upper()
        vehiculo = db.vehiculos.find_one({"patente": patente})

        if not vehiculo:
            print("No existe un vehículo con esa patente.")
            return []
        id_vehiculo = vehiculo["_id"]
        vehiculo_info = {
            "marca": vehiculo.get("marca"),
            "modelo": vehiculo.get("modelo"),
            "anio": vehiculo.get("anio"),
            "patente": vehiculo.get("patente"),
        }

    tipo = input("Tipo de póliza (Auto, Vida, Hogar, etc.): ").strip() or "Auto"
    prima_str = input("Prima mensual: ").strip()
    cobertura_str = input("Cobertura total: ").strip()

    try:
        prima_mensual = float(prima_str) if prima_str else 0.0
        cobertura_total = float(cobertura_str) if cobertura_str else 0.0
    except ValueError:
        print("Valores numéricos inválidos.")
        return []

    fecha_inicio = datetime.now()
    duracion_anios = 1
    fecha_fin = fecha_inicio + timedelta(days=365 * duracion_anios)

    estado = "Activa"

    ultimo = db.polizas.find_one(sort=[("_id", -1)])
    if ultimo and isinstance(ultimo["_id"], str) and ultimo["_id"].startswith("POL"):
        ult_num = int(ultimo["_id"].replace("POL", ""))
        nuevo_id = f"POL{ult_num + 1}"
    else:
        nuevo_id = "POL1001"

    nueva_poliza = {
        "_id": nuevo_id,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "tipo": tipo,
        "prima_mensual": prima_mensual,
        "cobertura_total": cobertura_total,
        "estado": estado,
        "id_cliente": id_cliente,
        "id_agente": id_agente,
        "id_vehiculo": id_vehiculo,
        "cliente_info": {
            "nombre_completo": f"{cliente['nombre']} {cliente['apellido']}",
            "activo": cliente.get("activo", True)
        },
        "agente_info": {
            "nombre_completo": agente["nombre"]
        },
        "vehiculo_info": vehiculo_info
    }

    db.polizas.insert_one(nueva_poliza)

    resumen = {
        "nro_poliza": nuevo_id,
        "tipo": tipo,
        "fecha_fin": fecha_fin,
        "estado": estado,
        "cobertura_total": cobertura_total,
        "cant_siniestros": 0
    }
    db.clientes.update_one(
        {"_id": id_cliente},
        {"$push": {"polizas_resumen": resumen}}
    )

    print(f"\nPóliza {nuevo_id} creada correctamente para {cliente['nombre']} {cliente['apellido']}.")
    return [nueva_poliza]

