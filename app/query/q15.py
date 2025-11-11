from database import db
from datetime import datetime, timedelta


def q15_emision_poliza_demo():
    cliente = db.clientes.find_one({"idCliente": "1"})
    agente = db.agentes.find_one({"idAgente": "101"})
    if not cliente or not agente:
        return [{"Error": "Cliente o agente no válido"}]

    nueva = {
        "nroPoliza": "POL9999",
        "idCliente": cliente["idCliente"],
        "tipo": "Hogar",
        "fechaInicio": datetime.now(),
        "fechaFin": datetime.now() + timedelta(days=365),
        "primaMensual": 30000,
        "coberturaTotal": 1000000,
        "idAgente": agente["idAgente"],
        "estado": "Activa"
    }
    db.polizas.insert_one(nueva)
    return [nueva]

