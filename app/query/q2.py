from database import db


def q2_siniestros_abiertos_cliente():
    pipeline = [
        {"$match": {"estado": "Abierto"}},
        {"$project": {
            "_id": 0,
            "Tipo": "$tipo",
            "Monto": "$monto_estimado",
            "cliente": "$cliente_afectado.nombre_completo"
        }}
    ]
    return list(db.siniestros.aggregate(pipeline))

