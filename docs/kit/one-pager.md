# Un curso sobre `proto_db`: el hueco que llena

**`proto_db`** es una base de datos de objetos transaccional, embebida en Python, construida
sobre colecciones inmutables y persistentes con compartición estructural. Cada commit publica una versión nueva
y todas las versiones anteriores siguen legibles; varios procesos pueden escribir el mismo espacio a través de un
clúster de pares con *lease* y *fencing*, en el que el almacenamiento nunca arbitra entre escritores. Se propone
como base de una serie de cursos, y se busca un colega que la co-construya.

## El hueco

Las fuentes son las del informe *Cursos de bases de datos comparables* (noviembre de 2026), citadas como allí.

- **MVCC como consecuencia, no como mecanismo agregado.** El 15-445 de CMU enseña MVCC sobre un motor de páginas
  mutables: cadenas de versiones, deltas de deshacer y recolección de basura
  ([Project 4](https://15445.courses.cs.cmu.edu/fall2025/project4/)); el viaje en el tiempo aparece como
  subproducto de MVCC sin recolección de basura
  ([notas de la clase 20](https://15445.courses.cs.cmu.edu/fall2025/notes/20-multiversioning.pdf)). La
  persistencia estructural se enseña como teoría de estructuras de datos
  ([MIT 6.851, L01](https://courses.csail.mit.edu/6.851/fall17/lectures/L01.html);
  [TUM FDS](https://www21.in.tum.de/teaching/FDS/SS18)). El informe no halló un curso que una las piezas.
- **Coordinación con *lease* y *fencing*.** Raft domina la enseñanza
  ([MIT 6.5840](https://pdos.csail.mit.edu/6.824/schedule.html)); en las notas de Cambridge 2025-26 no aparecen
  "lease" ni "fencing" ([notas](https://www.cl.cam.ac.uk/teaching/2526/ConcDisSys/dist-sys-notes.pdf)). *Lease*
  más *fencing* y los resultados de commit desconocidos son material de blogs y cursos comerciales
  ([Surfing Complexity](https://surfingcomplexity.blog/2025/03/03/locks-leases-fencing-tokens-fizzbee/)).
- **Bases inmutables o temporales.** El informe no halló ningún curso universitario cuyo tema sea Datomic, XTDB,
  TerminusDB o Dolt, solo capacitación de proveedor ([XTDB training](https://xtdb.com/support/training)), ni
  literatura pedagógica sobre cómo enseñarlas
  ([DataEd 2026](https://dataedinitiative.github.io/DataEd26/submission.html)).

Advertencia del propio informe: es un hecho sobre *títulos de programas*, no una prueba de que nadie lo enseñe.
Y un hueco pedagógico no implica demanda.

## Escala: no es la liga de Pavlo, es un complemento

El 15-445 tiene 25 clases en unas 15 semanas y cinco proyectos acumulativos sobre BusTub
([Syllabus](https://15445.courses.cs.cmu.edu/fall2025/syllabus.html),
[Assignments](https://15445.courses.cs.cmu.edu/fall2025/assignments.html)), con calificador automático en
Gradescope ([Project 4](https://15445.courses.cs.cmu.edu/fall2025/project4/)); BusTub tiene unas 5,2 mil estrellas
([BusTub](https://github.com/cmu-db/bustub)) y el canal del grupo, unos 90 mil suscriptores según rastreadores de
terceros ([developereducators](https://developereducators.com/channel/cmudatabasegroup/)). `proto_db` tiene un
mantenedor, ningún material público de curso y ningún calificador automático; los proyectos del ecosistema están
abiertos a revisión y no listos para producción. No compite con ese curso en amplitud. Ofrece un contraste: el
estudiante del 15-445 construye cadenas de versiones y su recolección; aquí vería que con compartición estructural
no hay cadena que reconstruir, y qué se paga a cambio (escritura por copia de camino). Haber visto el 15-445, o
sus clases en video, es un prerrequisito sensato (inferencia del informe).

## Cuatro cursos

| Curso | Contenido central |
|---|---|
| 1. La base de objetos pura | Inmutabilidad y compartición estructural; MVCC por construcción; lecturas "as of"; el ERP modelado; el costo de escritura |
| 2. Consultas y su optimización | Planes y costos; caché de resultados por puntero sin invalidación; paginación profunda por rango; residencia en memoria |
| 3. Distribución y coordinador | Clúster de pares con *lease* y *fencing*; la seguridad no depende del tiempo; resultados de commit desconocidos; ejercicios con errores plantados |
| 4. Registro de clases en tiempo de ejecución | **Pendiente del diseño del runtime.** No entra en ninguna propuesta mientras ese diseño siga abierto |

El ejemplo común es un ERP pequeño (usuarios, grupos, personas, oportunidades), pensado para unir al programador
de sistemas y al de aplicaciones, que ve primero qué permite cada propiedad y después cómo funciona. Es una
decisión de diseño del curso, sin contraparte en las fuentes: ningún programa leído declara dos públicos.

## Qué existe hoy

En el repositorio, en inglés: una guía de diseño escrita para enseñar (cada capítulo va del problema medido a la
solución obvia que falla, las alternativas medidas, la elección con su invariante y su costo, incluidos los
errores cometidos), un documento del protocolo del coordinador con diagramas de secuencia y de estado, y
tutoriales cuyo código ejecuta la batería de pruebas. El kit de reclutamiento (`docs/kit/`) suma una demo de 10
minutos que verifica sus propios resultados, un capítulo de muestra (residencia en memoria) y un ejercicio con un
error real plantado: un commit confirmado que se perdía, hallado por una revisión adversarial y corregido.

## Qué se busca

Un curso dictado solo por su autor es un curso del autor. Se busca un docente que lo co-construya: que revise el
temario, critique los materiales y, si le interesa, firme con el autor un artículo de artefacto educativo (el
informe señala el taller DataEd como vía).
