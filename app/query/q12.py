from database import db


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

