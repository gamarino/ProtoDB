"""
Ejemplo de Indexación de Colecciones en ProtoBase

Este script demuestra cómo las colecciones principales (List, Set, Dictionary)
pueden mantener sus propios índices para acelerar drásticamente las consultas.

Se contrasta una búsqueda lineal (lenta) con una búsqueda indexada (rápida)
sobre la misma colección de datos.
"""
import time
import os
import sys
# Ensure project root is on sys.path for direct execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.common import DBObject
from proto_db.queries import WherePlan
from proto_db.lists import List

# --- 1. Definición del Modelo de Datos y Configuración ---

class User(DBObject):
    """Un objeto de datos simple para nuestro ejemplo."""
    pass

# Configurar un entorno de base de datos en memoria.
storage = MemoryStorage()
object_space = ObjectSpace(storage=storage)
database = object_space.new_database('IndexExampleDB')
tr = database.new_transaction()

# --- 2. Creación y Población de la Colección ---

print("Creando una colección con 50,000 usuarios...")
user_list = tr.new_list()
num_users = 50000
for i in range(num_users):
    user = User(
        id=i,
        name=f'User {i}',
        email=f'user.{i}@example.com',
        city='City ' + str(i % 100) # Distribuir en 100 ciudades
    )
    user_list = user_list.append_last(user)

print(f"Colección creada con {user_list.count} usuarios.\n")

# El email que buscaremos, uno que está casi al final para maximizar el tiempo de escaneo lineal.
target_email = f'user.{num_users - 5}@example.com'

# ======================================================================
# --- Escenario 1: Búsqueda sin índice (Escaneo Lineal) ---
# ======================================================================
print("--- Escenario 1: Búsqueda sin índice (Escaneo Lineal) ---")
print(f"Buscando al usuario con email = '{target_email}' en la lista original.")

# Convertimos la lista en un plan de consulta. Como no tiene índices,
# esto crea un ListQueryPlan simple.
unindexed_plan = user_list.as_query_plan()

# Creamos un WherePlan. El optimizador no encontrará índices en el plan base.
search_plan_slow = WherePlan(
    filter_spec=['email', '==', target_email],
    based_on=unindexed_plan,
    transaction=tr
)

start_time = time.perf_counter()
results_slow = list(search_plan_slow.execute())
end_time = time.perf_counter()

time_slow = (end_time - start_time) * 1000  # en milisegundos

print(f"Usuario encontrado: {results_slow[0].name if results_slow else 'Ninguno'}")
print(f"Tiempo de ejecución (sin índice): {time_slow:.4f} ms")
print("-> El motor tuvo que escanear la lista completa para encontrar el resultado.\n")

# ======================================================================
# --- Escenario 2: Búsqueda con Índice (Acceso Optimizado) ---
# ======================================================================
print("--- Escenario 2: Búsqueda con Índice (Acceso Optimizado) ---")
print("Primero, creamos una versión indexada de la lista en el campo 'email'.")

# Usamos el método `add_index` en nuestra lista. Esto es una operación inmutable
# y devuelve una nueva instancia de la lista que contiene el índice.
indexed_user_list = user_list.add_index('email')

print(f"Buscando al usuario con email = '{target_email}' en la lista indexada.")

# Ahora, convertimos la lista *indexada* en un plan de consulta.
# Esto devolverá un `IndexedQueryPlan` que contiene el índice de 'email'.
indexed_plan = indexed_user_list.as_query_plan()

# Creamos un WherePlan exactamente igual que antes.
search_plan_fast = WherePlan(
    filter_spec=['email', '==', target_email],
    based_on=indexed_plan,
    transaction=tr
)

start_time = time.perf_counter()
results_fast = list(search_plan_fast.execute())
end_time = time.perf_counter()

time_fast = (end_time - start_time) * 1000  # en milisegundos

print(f"Usuario encontrado: {results_fast[0].name if results_fast else 'Ninguno'}")
print(f"Tiempo de ejecución (con índice): {time_fast:.4f} ms")
print("-> El motor detectó y usó el índice 'email' para una búsqueda directa (O(log N)).\n")


# ======================================================================
# --- Conclusión ---
# ======================================================================
print("--- Conclusión ---")
if time_fast > 0:
    speedup = time_slow / time_fast
    print(f"La búsqueda indexada fue {speedup:.2f} veces más rápida que el escaneo lineal.")
else:
    print("La búsqueda indexada fue prácticamente instantánea.")
