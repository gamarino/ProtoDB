#!/bin/bash
M=/home/gamarino/Documentos/proyectos/scratch-8.3n/m8-raw-path
PY=/home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python
until grep -q DONE /home/gamarino/Documentos/proyectos/scratch-8.3n/credit-6b0697a/driver.log 2>/dev/null; do sleep 30; done
for rep in 1 2; do
 for c in 2d78c2b 6b0697a; do
  echo "$(date -Is) start $c r$rep load $(cut -d' ' -f1-3 /proc/loadavg)" >> $M/driver.log
  cd $M/export-$c
  systemd-run --user --scope -q -p MemoryMax=12G -p MemorySwapMax=0 $PY -X gil=0 -m benchmarks --workdir $M/work-$c-r$rep --json $M/$c-gil0-r$rep.json > $M/$c-gil0-r$rep.log 2>&1
  echo "$(date -Is) end $c r$rep rc=$? load $(cut -d' ' -f1-3 /proc/loadavg)" >> $M/driver.log
 done
done
echo DONE >> $M/driver.log
