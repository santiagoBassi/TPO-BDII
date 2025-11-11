from database import db


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

