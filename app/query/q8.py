from database import db
from datetime import datetime, timedelta


def q8_siniestros_accidente_ultimo_anio():
    hace_un_anio = datetime.now() - timedelta(days=365)
    pipeline = [
        {
            "$match": {
                "tipo": "Accidente",
                "fecha": {"$gte": hace_un_anio}
            }
        },
        {
            "$project": {
                "_id": 0,
                "Siniestro": "$_id",
                "Fecha": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$fecha"
                    }
                },
                "Monto": "$monto_estimado",
                "Descripcion": "$descripcion",
                "Estado": "$estado"
            }
        }
    ]
    return list(db.siniestros.aggregate(pipeline))

