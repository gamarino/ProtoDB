#!/bin/bash
# Interleaved save-path probe: 12 reps x 8 variants, alternating order each rep.
R=/home/gamarino/Documentos/proyectos/scratch-8.3n/regr
PY=/home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python
PROBE=/home/gamarino/Documentos/proyectos/ProtoBase/benchmarks/save_path_probe.py
V=("4aab0f2:" "7ef1636:" "700d48f:" "550dde5:" "ec90c25:" "0eed600:" "c953d7e:" "c953d7e:class_defines_fast")
OUT=$R/save/save-probe.jsonl
for rep in $(seq 0 11); do
  if (( rep % 2 )); then order=$(seq 7 -1 0); else order=$(seq 0 7); fi
  for i in $order; do
    c=${V[$i]%%:*}; p=${V[$i]#*:}
    b0=$(awk '/^cpu /{print $2+$3+$4+$7+$8}' /proc/stat)
    line=$(cd $R/exp/$c && $PY -X gil=0 $PROBE --commits 400 --batches 5 ${p:+--patch $p})
    b1=$(awk '/^cpu /{print $2+$3+$4+$7+$8}' /proc/stat)
    echo "{\"rep\": $rep, \"commit\": \"$c\", \"busy_jiffies_machine\": $((b1-b0)), \"result\": $line}" >> $OUT
  done
done
for c in 4aab0f2 7ef1636 ec90c25 0eed600; do
  (cd $R/exp/$c && $PY -X gil=0 $PROBE --commits 400 --batches 1 --profile $R/save/profile-$c.txt > /dev/null)
done
(cd $R/exp/c953d7e && $PY -X gil=0 $PROBE --commits 400 --batches 1 --profile $R/save/profile-c953d7e.txt > /dev/null)
echo finished
