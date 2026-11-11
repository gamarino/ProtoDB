# Cluster commit regression and GIL-on starvation (R136), 2026-11-11

The result data behind every figure in the R136 investigation report. Copied from the scratch directory
`scratch-8.3n/regr/` without changes, except that the JSON files are re-serialized without indentation; every run
record is kept. Measured trees are clean `git archive` exports (4aab0f2 .. c953d7e; the probes are run from
7757fb3, whose product code is c953d7e). Scripts: `benchmarks/bisect_commit_path.py`,
`benchmarks/save_path_probe.py` (5b50252, patches in 7757fb3), `benchmarks/gil_ack_probe.py`,
`benchmarks/scan_state_probe.py` (7757fb3).

| Figure | File |
|---|---|
| Run log: start, stop, swap of every run; foreign load; stage 1 stopped and superseded; stage 3 stopped at 7 rounds | `driver.log` |
| Q1 cluster bisection, 16 exports x 2 cells x 14 paired rounds (448 kept, 62 discarded, 0 failed): rates, latency, critical-section stages, syncs, frame bytes, lease attempts, pipeline counters, foreign CPU per run; per-export medians and neighbour ratios in `summary` | `stage2-chain.json` (progress log `stage2-chain.log`) |
| Q1 span ratios and sign tests (4aab0f2 -> c953d7e 0.898 / 0.846; 8.3m 0.967 / 0.919; 550dde5 -> ec90c25 0.993 / 0.987; ec90c25 -> 0eed600 0.965 / 0.936), computed from `stage2-chain.json` | `stage2-pairs.txt` |
| Q1 single-process build per commit (+14.8 %, +7.6 %, +19.5 %, total +40 %) | `save/save-probe.jsonl`, `save/save-summary.txt` (driver `save/run_save.sh`) |
| Q1 profiles (`class_defines` 155,488 calls; residency bookkeeping) | `save/profile-<commit>.txt` |
| Q2 patches: `class_defines` prefilter 0.85x, level with ec90c25 (0.986x); cached pointer hash 1.01x at c953d7e, 0.912x at 4aab0f2 | `save/save-probe-patches.jsonl`, `save/save-patches-summary.txt` (driver `save/run_save2.sh`) |
| Q2 cluster check of the prefilter (+5.6 %, 7/7 and 6/7 rounds; 0.972 / 0.947 of ec90c25), 7 rounds | `stage3-cdfast.json`, `stage3-cdfast.log` |
| Q3 commits/s, reads/s and ack-path timings (GIL take-back, lane wait and run, fsync, send wait, sendall) per variant and node | `q3/gil-ack-probe.json`, `q3/run.log` |
| Q4 alone: speedups 0.99 / 1.69 / 3.16 / 3.72 / 4.28; probe state and variants | `q4/alone-gil-off.json`, `q4/alone.log` |
| Q4 after the suite's preceding workloads: 4.13x at 16 workers; RSS 1,815 MB, 2,082,100 tracked objects, 1.652 s full collection, 0 collections during scans | `q4/suite-prefix-gil-off.json`, `q4/suite.log` (driver `q4/run_q4.sh`) |

`stage2-chain.json` runs carry `discarded: true` when other processes used 2.5 CPUs or more during the run; those
are excluded from every figure. The export `c953d7e-cdfast` in `stage3-cdfast.json` is an experimental, uncommitted
copy of c953d7e with the `class_defines` prefilter (its `commit` field says so).
