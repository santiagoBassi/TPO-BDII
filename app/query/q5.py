from database import db


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

