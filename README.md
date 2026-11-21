# Shopping Sessions (Builders Buildathon, Databricks track)

A single DuckDB pipeline in Python that reads the dirty click-stream CSVs in `/data` and writes the two daily Parquet tables `user_daily` and `campaign_daily`, following `rules.md` exactly.

## How it works

1. Input lines. Each CSV file is first scanned for a `\r` byte. Files without one go through DuckDB's CSV reader with quoting turned off, so every comma splits a field and wrong-length or empty lines are dropped. Each field must then either have no quote or be wrapped whole in quotes. Files that contain a `\r` are split on `\n` by hand instead, because DuckDB would treat the `\r` as a line end.
2. Linked IDs. Link lines become graph edges, and `scipy.sparse.csgraph.connected_components` finds each person. The person's name is the byte-wise smallest ID.
3. Events. Duplicates are resolved by the smallest `ingest_ts` before validation, then timestamps (F1, F2, F3), window, type, product and price are checked and late events dropped.
4. Sessions. A running sum over each person's events by time starts a new session after a gap over 30 minutes. Equal timestamps always share a session.
5. Credit. An ASOF join finds the latest qualifying click per session (ties go to the smallest `click_id`).
6. Assists. Each person's clicks for one campaign are merged into non-overlapping 7-day coverage intervals, and each interval is matched to the run of sessions it covers. Only real (session, campaign) pairs are ever produced, so whale users and shared devices stay cheap.

## Run

```
docker build -t shop .
docker run --rm -v $PWD/data:/data:ro -v $PWD/out:/output -v $PWD/scratch:/scratch shop
```

## Tests

`make fixtures` builds the image and runs the 36 official edge-case fixtures; `make dev`, `make run-dev` and `make check-dev ANSWER=dev-answer` run the official 1M-event dev check. `Makefile`, `scripts/` and `fixtures/` come unchanged from the organizers' DuckDB starter. All 36 fixtures pass and the dev set matches the published answer exactly.

`tests/reference.py` is a slow, literal Python reading of `rules.md`. `tests/mkdata.py` makes small, very dirty datasets aimed at the edge cases (quoting, `\r`, malformed lines, every bad timestamp form, offsets, duplicates with a bad first copy, late events at exactly 24 h, long link chains, a shared device, upper-case IDs, boundary clicks at 7 days and 7 days + 1 ms, ties on click time). `tests/run_all.sh` runs both and compares the outputs. `tests/compare.py` also checks against the official `dev-answer` CSVs.

## Third-party code

DuckDB, NumPy, SciPy and PyArrow (open-source libraries, pinned in the Dockerfile). The test harness files listed above are from the organizers' starter. No code was written before the event.

## License

MIT, see `LICENSE`.
