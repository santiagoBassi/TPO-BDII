from database import db


def q6_polizas_vencidas_con_cliente():
    pipeline = [
        {"$match": {"estado": "Vencida"}},
        {
            "$project": {
                "_id": 0,
                "Poliza": "$_id",
                "Fecha de Inicio": {
                    "$dateToString": {
                        "format": "%d-%m-%Y", 
                        "date": "$fecha_inicio"
                    }
                },
                "Fecha de Vencimiento": {
                    "$dateToString": {
                        "format": "%d-%m-%Y",  
                        "date": "$fecha_fin"
                    }
                },
               
                "prima mensual": "$prima_mensual",
                "cobertura total": "$cobertura_total",
                "estado": "$estado",
                "Cliente": "$cliente_info.nombre_completo"
            }
        }
    ]
    return list(db.polizas.aggregate(pipeline))

