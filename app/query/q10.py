from database import db


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

