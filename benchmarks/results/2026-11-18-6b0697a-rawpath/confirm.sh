#!/bin/bash
M=/home/gamarino/Documentos/proyectos/scratch-8.3n/m8-raw-path
PY=/home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python
for rep in 1 2 3; do
 for c in 2d78c2b 6b0697a; do
  echo "$(date -Is) start $c c$rep load $(cut -d' ' -f1-3 /proc/loadavg)" >> $M/confirm.log
  cd $M/export-$c
  systemd-run --user --scope -q -p MemoryMax=12G -p MemorySwapMax=0 $PY -X gil=0 -m benchmarks --only inventory cold_open query_cache cluster_cache cluster_store_latency topk point_lookup --workdir $M/cwork-$c-c$rep --json $M/$c-confirm-c$rep.json > $M/$c-confirm-c$rep.log 2>&1
  echo "$(date -Is) end $c c$rep rc=$? load $(cut -d' ' -f1-3 /proc/loadavg)" >> $M/confirm.log
 done
done
echo DONE >> $M/confirm.log
