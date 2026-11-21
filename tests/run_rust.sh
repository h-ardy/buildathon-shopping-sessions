#!/bin/bash
# Cross-check the Rust binary against reference.py on many dirty synthetic datasets.
cd "$(dirname "$0")/.."
BIN=${BIN:-./target/release/shop}
fail=0
for s in $(seq 1 ${1:-30}); do for cr in "" 1; do
  d=/tmp/rt/d$s$cr; r=/tmp/rt/r$s$cr; o=/tmp/rt/x$s$cr; rm -rf $d $r $o; mkdir -p /tmp/rt
  NOCR=$cr python3 tests/mkdata.py $d $s 80 && python3 tests/reference.py $d $r
  DATA_DIR=$d OUTPUT_DIR=$o NCPU=2 $BIN 2>/dev/null || { echo "crash seed $s"; fail=1; continue; }
  res=$(python3 tests/compare.py $o $r | tail -1); [ "$res" = PASS ] || { echo "seed $s cr=$cr FAIL"; fail=1; }
done; done
[ $fail = 0 ] && echo "ALL PASS" || echo "SOME FAILED"
