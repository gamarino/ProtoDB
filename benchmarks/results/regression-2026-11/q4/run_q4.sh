#!/bin/bash
# Q4: parallel_scan + scan_state_probe alone, then after the suite's preceding workloads (GIL off), at 7757fb3 (product = c953d7e).
R=/home/gamarino/Documentos/proyectos/scratch-8.3n/regr
PY=/home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python
cd $R/exp/probes-head
echo "$(date -Is) alone start"
$PY -X gil=0 -m benchmarks --plugin benchmarks.scan_state_probe --only parallel_scan scan_state_probe --workdir $R/q4/work-alone --json $R/q4/alone-gil-off.json > $R/q4/alone.log 2>&1
echo "$(date -Is) alone exit $?"
$PY -X gil=0 -m benchmarks --plugin benchmarks.scan_state_probe --only point_lookup range_query topk deep_page query_cache inventory bulk_insert index_update_cost commit_durability versions canonical_rewalk cold_open concurrent_commits concurrent_reads parallel_scan scan_state_probe --workdir $R/q4/work-suite --json $R/q4/suite-prefix-gil-off.json > $R/q4/suite.log 2>&1
echo "$(date -Is) suite prefix exit $?"
