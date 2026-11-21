#!/bin/bash
# Cross-check pipeline.py against reference.py on many dirty synthetic datasets.
cd "$(dirname "$0")/.."
fail=0
for s in $(seq 1 ${1:-12}); do for cr in "" 1; do
  d=/tmp/rt/d$s$cr; o=/tmp/rt/o$s$cr; r=/tmp/rt/r$s$cr; rm -rf $d $o $r; mkdir -p /tmp/rt
  NOCR=$cr python3 tests/mkdata.py $d $s 80 && python3 tests/reference.py $d $r
  DATA_DIR=$d OUTPUT_DIR=$o SCRATCH_DIR=/tmp/rt/s NCPU=2 python3 pipeline.py 2>/tmp/rt/log || { echo "pipeline crashed seed $s"; tail -3 /tmp/rt/log; fail=1; continue; }
  res=$(python3 tests/compare.py $o $r | tail -1); [ "$res" = PASS ] || { echo "seed $s nocr=$cr FAIL"; fail=1; }
done; done
[ $fail = 0 ] && echo "ALL PASS" || echo "SOME FAILED"
