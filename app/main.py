from queries import *
from pprint import pprint

queries = [
        (1, "Clientes activos con pólizas vigentes", q1_clientes_activos_polizas_vigentes),
        (2, "Siniestros abiertos con tipo, monto y cliente afectado", q2_siniestros_abiertos_cliente),
        (3, "Vehículos asegurados con su cliente y póliza", q3_vehiculos_con_cliente_poliza),
        (4, "Clientes sin pólizas activas", q4_clientes_sin_polizas_activas),
        (5, "Agentes activos con cantidad de pólizas asignadas", q5_agentes_activos_cant_polizas),
        (6, "Pólizas vencidas con el nombre del cliente", q6_polizas_vencidas_con_cliente),
        (7, "Top 10 clientes por cobertura total", q7_top10_clientes_cobertura_total),
        (8, "Siniestros tipo Accidente del último año", q8_siniestros_accidente_ultimo_anio),
        (9, "Pólizas activas ordenadas por fecha de inicio", q9_polizas_activas_ordenadas),
        (10, "Pólizas suspendidas con estado del cliente", q10_polizas_suspendidas_estado_cliente),
        (11, "Clientes con más de un vehículo asegurado", q11_clientes_multiples_vehiculos),
        (12, "Agentes y cantidad de siniestros asociados", q12_agentes_cant_siniestros),
        (13, "ABM Clientes (ejemplo alta)", q13_abm_clientes_demo),
        (14, "Alta de nuevos siniestros", q14_alta_siniestro_demo),
        (15, "Emisión de nuevas pólizas", q15_emision_poliza_demo),
    ]

def print_options():
    print("Seleccione una consulta para ejecutar:")
    for query in queries:
        print(f"{query[0]}. {query[1]}")

def get_query(choice):
    if 1 <= choice <= len(queries):
        return queries[choice - 1][2]
    else:
        return None


def main():
    print("Bienvenido al sistema de gestión de seguros.")
    
    

    while(True):
        print_options()
       
        choice = int(input("Ingrese el número de la consulta a ejecutar (0 para salir): "))
        if choice == 0:
            print("Saliendo del sistema. ¡Hasta luego!")
            break
        selected_query = get_query(choice)
        if selected_query:
            result = selected_query()
            pprint(result, sort_dicts=False)
        else:
            print("Opción no válida. Por favor, intente de nuevo.")
    
if __name__ == "__main__":
    main()
