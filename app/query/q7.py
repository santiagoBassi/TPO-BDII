from database import db


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

