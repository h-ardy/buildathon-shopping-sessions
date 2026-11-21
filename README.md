# Shopping Sessions (Builders Buildathon, Databricks track)

A native Rust pipeline that reads the dirty click-stream CSVs in `/data` and writes the two daily Parquet tables `user_daily` and `campaign_daily`, following `rules.md` exactly. `duckdb/pipeline.py` is the first, slower DuckDB version of the same rules, kept as a second implementation to check against.

## How it works

1. Reading. Every CSV is cut into 8 MB slices. Each thread reads its own slice with `pread` and parses it straight away, so disk reads and parsing overlap. A slice owns the lines that start inside it, and finishes its last line by reading past its end.
2. Lines. One AVX2 pass marks every comma, quote and newline (with a scalar fallback). A line is kept only with the exact field count and quotes around whole fields. `\r` is an ordinary character.
3. Linked IDs. Link pairs go through union-find, and each person is named by the byte-wise smallest ID. An open-addressing table maps each linked ID to its person.
4. Events. Records are bucketed by a hash of `event_id` with one counting sort. Each bucket keeps the copy with the earliest `ingest_ts`, and only then validates it (window, type, product, price, late cutoff). Hash collisions between different IDs are compared byte for byte.
5. Sessions, credit and assists. Events and clicks are bucketed by person, then sorted by time. One pass per person builds the sessions, credits each session to the latest click within 7 days (smallest `click_id` on ties), and keeps a sliding 7-day window of campaign counts for assists.
6. Output. `campaign_daily` is one file. `user_daily` is written as several files in parallel.

Large buffers are mapped directly with `mmap`. That cuts allocator overhead on the several GB the run touches.

## Checks

- `make fixtures`: the 36 official edge-case fixtures (all pass).
- `make dev`, `make run-dev`, `make check-dev ANSWER=dev-answer`: the official 1M-event dev set (exact match).
- `tests/run_rust.sh`: makes small, very dirty datasets with `tests/mkdata.py` and compares the binary with `tests/reference.py`, a slow, literal Python reading of `rules.md`. `SHOP_CHUNK=1` forces 1-byte slices to test slice boundaries. `SHOP_NOSIMD=1` tests the scalar parser.
- On a 20M-event set from the official generator, the Rust and DuckDB outputs are identical.

## Third-party code

Crates `parquet`, `rayon`, `memchr`, `rustc-hash` and `libc` (versions pinned in `Cargo.lock`). DuckDB, NumPy, SciPy and PyArrow for the DuckDB version and the tests. `Makefile`, `scripts/` and `fixtures/` come from the organizers' DuckDB starter. No code was written before the event.

## License

MIT, see `LICENSE`.
