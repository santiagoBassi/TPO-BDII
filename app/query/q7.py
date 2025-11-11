from database import db


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

