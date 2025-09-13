# ProtoBase: The In-Memory Python Database that Remembers

Lema: Experience the speed of in-memory computing with the safety of on-disk persistence. ProtoBase offers fully transactional Python objects that operate at RAM speed.

---

## Core Concept

ProtoBase es una plataforma de datos transaccionales de alto rendimiento para Python. Su arquitectura híbrida opera directamente sobre objetos nativos de Python a velocidad de memoria usando copy‑on‑write, mientras un motor de persistencia desacoplado garantiza la durabilidad en segundo plano. Olvídate de ORMs complejos y de la latencia de disco en la ruta crítica. Con ProtoBase, trabajas con la simplicidad de un `dict` o una `list`, pero con garantías ACID.

## Key Characteristics

- Near In-Memory Performance
  - Lecturas y escrituras dentro de una transacción se ejecutan en memoria sobre estructuras inmutables (copy‑on‑write).
  - La persistencia se realiza en hilos de fondo, desacoplando la latencia de tu app de la velocidad del disco.

- Intelligent Write-Through Cache
  - Los datos recién escritos se publican inmediatamente en el AtomObjectCache y AtomBytesCache.
  - Elimina la penalización de “leer tus propias escrituras” y mantiene “caliente” lo más reciente para lecturas a velocidad de RAM.

- Advanced Query Optimizer
  - El motor usa índices para `AND`, `OR` y términos únicos, construyendo planes como AndMerge, OrMerge o IndexedSearchPlan.
  - En lugar de escanear tablas, combina conjuntos de referencias de bajo costo y materializa objetos sólo al final.

- Full ACID Transactions with Snapshot Isolation
  - Aislamiento por instantáneas gracias a estructuras inmutables: lecturas consistentes dentro de cada transacción.
  - Sin lecturas sucias ni no‑repetibles.

- Zero-Overhead Object Mapping
  - Trabaja directamente con tus objetos de Python: sin clases base obligatorias, sesiones de ORM ni DSLs adicionales.
  - Si es un objeto de Python, puede persistirse.

## Ideal Use Cases

- Aplicaciones de Alta Tasa de Transacción
  - Carritos de compra, gestión de sesiones, leaderboards de juegos, telemetría.

- Modelado de Datos Complejos
  - Grafos de objetos: documentos anidados, redes sociales, sistemas de configuración.

- Caché Persistente y Transaccional
  - Reemplazo de alto nivel para Redis/Memcached cuando se requieren transacciones multi‑clave y durabilidad automática.

- Prototipado y Desarrollo Rápido
  - Cuando `pickle`/`shelve` se quedan cortos pero Postgres/MySQL son una sobrecarga administrativa.

## Simple Analogy

Piense en ProtoBase como un super `dict` de Python. Es tan fácil de usar como un diccionario, pero es thread‑safe, totalmente transaccional, y guarda su estado en disco de forma asíncrona sin que tengas que preocuparte. Es el puente perfecto entre la simplicidad de las estructuras de datos de Python y la robustez de una base de datos real.

---

### Why it feels instant

- Copy‑on‑write sobre un grafo de objetos en memoria: la hebra principal no espera E/S.
- Persistencia y serialización delegadas a un executor pool; `commit` sincroniza con el WAL.
- Write‑through cache: lecturas después de escribir impactan en caché, no en disco.

### Why it stays safe

- Transacciones ACID con aislamiento por snapshot.
- Índices secundarios inmutables y optimizador que evita materializaciones tempranas.
- AtomPointer como identificador estable para intersecciones y deduplicación en planes `AND`/`OR`.

---

## Positioning Statement

ProtoBase ya no es “sólo” una base de datos de objetos persistente: es una Plataforma de Datos Transaccionales con Rendimiento de Memoria. Reúne la velocidad y ergonomía de una solución en memoria con la durabilidad, las consultas y las garantías transaccionales de una base de datos tradicional—directamente en Python.