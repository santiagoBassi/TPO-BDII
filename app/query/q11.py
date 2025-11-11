from database import db


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

