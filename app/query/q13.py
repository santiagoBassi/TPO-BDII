from database import db


def q13_abm_clientes_demo():
    nuevo = {
        "idCliente": "999",
        "nombre": "Demo",
        "apellido": "Cliente",
        "dni": "12345678",
        "email": "demo@cliente.com",
        "telefono": "11112222",
        "direccion": "Av. Siempre Viva 123",
        "ciudad": "Springfield",
        "provincia": "Buenos Aires",
        "activo": True
    }
    db.clientes.insert_one(nuevo)
    return [nuevo]

