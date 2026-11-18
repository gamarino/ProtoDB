#!/bin/bash
R=/home/gamarino/Documentos/proyectos/scratch-8.3n/credit-6b0697a
PY=/home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python
cd $R/export-6b0697a
for rep in 1 2 3 4 5; do
 for reads in none credit-limit; do
  for gil in 0 1; do
   for mode in direct intents; do
    tag=$mode-$reads-gil$gil-r$rep
    echo "$(date -Is) start $tag load $(cut -d' ' -f1-3 /proc/loadavg)" >> $R/driver.log
    systemd-run --user --scope -q -p MemoryMax=8G -p MemorySwapMax=0 $PY -X gil=$gil -m benchmarks.semantic.credit_experiment --mode $mode --reads $reads --threads 8 --orders 400 --customers 8 --products 4 --seed 1 --workdir $R/work-$tag --out $R/$tag.json > $R/$tag.log 2>&1
    echo "$(date -Is) end $tag rc=$?" >> $R/driver.log
   done
  done
 done
done
echo DONE >> $R/driver.log
