from database import db


def q9_polizas_activas_ordenadas():
    return list(db.polizas.find(
        {"estado": "Activa"},
        {"_id": 0, "Poliza": "$nroPoliza", "FechaInicio": "$fechaInicio", "FechaFin": "$fechaFin"}
    ).sort("fechaInicio", 1))

