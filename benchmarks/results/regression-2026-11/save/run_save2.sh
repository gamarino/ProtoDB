#!/bin/bash
# Patch experiments on c953d7e, plus 4aab0f2 as reference: 10 reps, alternating order.
R=/home/gamarino/Documentos/proyectos/scratch-8.3n/regr
PY=/home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python
PROBE=/home/gamarino/Documentos/proyectos/ProtoBase/benchmarks/save_path_probe.py
V=("4aab0f2:" "c953d7e:" "c953d7e:class_defines_fast" "c953d7e:class_defines_fast,pointer_hash_cached" "4aab0f2:pointer_hash_cached")
OUT=$R/save/save-probe-patches.jsonl
for rep in $(seq 0 9); do
  if (( rep % 2 )); then order=$(seq 4 -1 0); else order=$(seq 0 4); fi
  for i in $order; do
    c=${V[$i]%%:*}; p=${V[$i]#*:}
    line=$(cd $R/exp/$c && $PY -X gil=0 $PROBE --commits 400 --batches 5 ${p:+--patch $p})
    echo "{\"rep\": $rep, \"commit\": \"$c\", \"result\": $line}" >> $OUT
  done
done
echo finished
