from database import db


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

