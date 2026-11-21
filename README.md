# Shopping Sessions (Builders Buildathon, Databricks track)

A native Rust pipeline that reads the dirty click-stream CSVs in `/data` and writes the two daily Parquet tables `user_daily` and `campaign_daily`, following `rules.md` exactly. `duckdb/pipeline.py` is the first, slower DuckDB version of the same rules, kept as a second implementation to check against.

## How it works

1. Reading. Every CSV is memory-mapped (no copy) and cut into 8 MB slices, which the threads parse in parallel. A slice owns the lines that start inside it and finishes its last line past its end. Each slice is faulted in with one `MADV_POPULATE_READ` call instead of one page fault per 4 KB.
2. Lines. One AVX2 pass marks every comma, quote and newline (with a scalar fallback). A line is kept only with the exact field count and quotes around whole fields. `\r` is an ordinary character.
3. Linked IDs. IDs are interned in parallel across 64 hash shards, then joined with union-find, and each person is named by the byte-wise smallest ID. An open-addressing table (one cache line per slot, ID bytes inline, at most 3/8 full so a lookup for an unlinked ID usually ends in the first slot or two) maps each linked ID to its person. This step runs at the same time as events pass 1 below, so its serial steps don't leave threads idle.
4. Events, in two passes. Pass 1 hashes only each line's `event_id` and finds the ids that occur more than once. Pass 2 parses fully: an event whose id occurs once is validated (window, type, product, price, late cutoff) and kept right away. The few repeated ids go to an exact dedup that keeps the copy with the earliest `ingest_ts`, compares ids byte for byte, and only then validates. Events wait in batches of 32 so the duplicate-id bit lookups are prefetched, and person lookups run in batches with the table slots prefetched ahead, so their cache misses overlap.
Clicks are parsed and bucketed while the events go through pass 2, dedup and bucketing, again so the two phases fill each other's serial steps.
5. Sessions, credit and assists. Events and clicks are bucketed by a hash of the person, each parsed slice in place through a reused buffer (no second full-size copy); a bucket gathers its pieces from every slice into one small buffer, and inside a bucket a small hash table groups them per person, and each person's events are sorted by time. One pass per person builds the sessions, credits each session to the latest click within 7 days (smallest `click_id` on ties), and keeps a sliding 7-day window of campaign counts for assists.
6. Output. `campaign_daily` is one file. `user_daily` is written as several files in parallel, with all user IDs of a file in one shared buffer instead of one allocation per row.

Large buffers are mapped directly with `mmap` and ask for transparent huge pages (`SHOP_ALLOC=0` turns that off), and keys are compared inline rather than through library calls. User and person IDs of up to 15 bytes are kept inline in each record, so grouping never reads back into the input files. Per-person grouping compares only the 64-bit person hash, and falls back to comparing names if two different names ever share a hash (`SHOP_EXACT=1` forces that path for testing).

## Checks

- `make fixtures`: the 36 official edge-case fixtures (all pass).
- `make dev`, `make run-dev`, `make check-dev ANSWER=dev-answer`: the official 1M-event dev set (exact match).
- `tests/run_rust.sh`: makes small, very dirty datasets with `tests/mkdata.py` and compares the binary with `tests/reference.py`, a slow, literal Python reading of `rules.md`. `SHOP_CHUNK=1` forces 1-byte slices to test slice boundaries. `SHOP_NOSIMD=1` tests the scalar parser.
- On a 20M-event set from the official generator, the Rust and DuckDB outputs are identical.

## Third-party code

Crates `parquet`, `rayon`, `memchr`, `rustc-hash` and `libc` (versions pinned in `Cargo.lock`). DuckDB, NumPy, SciPy and PyArrow for the DuckDB version and the tests. `Makefile`, `scripts/` and `fixtures/` come from the organizers' DuckDB starter. No code was written before the event.

## License

MIT, see `LICENSE`.
