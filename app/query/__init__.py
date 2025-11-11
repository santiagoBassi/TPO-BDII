"""
Módulo de queries del sistema de gestión de seguros.
Cada query está en su propio archivo para facilitar el mantenimiento.
"""

# Importar todas las queries
from .q1 import q1_clientes_activos_polizas_vigentes
from .q2 import q2_siniestros_abiertos_cliente
from .q3 import q3_vehiculos_con_cliente_poliza
from .q4 import q4_clientes_sin_polizas_activas
from .q5 import q5_agentes_activos_cant_polizas
from .q6 import q6_polizas_vencidas_con_cliente
from .q7 import q7_top10_clientes_cobertura_total
from .q8 import q8_siniestros_accidente_ultimo_anio
from .q9 import q9_polizas_activas_ordenadas
from .q10 import q10_polizas_suspendidas_estado_cliente
from .q11 import q11_clientes_multiples_vehiculos
from .q12 import q12_agentes_cant_siniestros
from .q13 import q13_abm_clientes_demo
from .q14 import q14_alta_siniestro_demo
from .q15 import q15_emision_poliza_demo

__all__ = [
    'q1_clientes_activos_polizas_vigentes',
    'q2_siniestros_abiertos_cliente',
    'q3_vehiculos_con_cliente_poliza',
    'q4_clientes_sin_polizas_activas',
    'q5_agentes_activos_cant_polizas',
    'q6_polizas_vencidas_con_cliente',
    'q7_top10_clientes_cobertura_total',
    'q8_siniestros_accidente_ultimo_anio',
    'q9_polizas_activas_ordenadas',
    'q10_polizas_suspendidas_estado_cliente',
    'q11_clientes_multiples_vehiculos',
    'q12_agentes_cant_siniestros',
    'q13_abm_clientes_demo',
    'q14_alta_siniestro_demo',
    'q15_emision_poliza_demo',
]

