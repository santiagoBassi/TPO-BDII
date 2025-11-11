from database import db


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

