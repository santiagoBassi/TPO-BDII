from database import db
from datetime import datetime


def q14_alta_siniestro_demo():
    nuevo = {
        "idSiniestro": "9999",
        "nroPoliza": "POL1001",
        "fecha": datetime.now(),
        "tipo": "Accidente",
        "montoEstimado": 150000.0,
        "descripcion": "Choque demo",
        "estado": "Abierto"
    }
    db.siniestros.insert_one(nuevo)
    return [nuevo]

