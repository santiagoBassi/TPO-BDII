from database import db


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

