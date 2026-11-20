# Shopping Sessions: a fast exact pipeline (Builders Buildathon, Databricks track)

A single Rust binary with no third-party crates. It reads `/data` and writes `/output/user_daily/` and
`/output/campaign_daily/` as Parquet, following `rules.md` exactly.

How it works:
- CSV files are read in parallel byte ranges (pread).
- IDs of up to 15 bytes are packed into exact `u128` keys, so their order is byte order. Longer IDs are interned.
- Linked IDs are joined with union-find.
- Events are deduplicated in 256 hash buckets, then regrouped by person, sessionized and attributed.
- Assists are counted with merged 7-day click intervals per campaign.
- The Parquet writer is a small built-in one: PLAIN encoding, uncompressed.

Local checks:
- `tools/run_fixtures.py` on the 36 fixtures;
- `tools/compare.py` against the published dev answer;
- `tools/docker_run.sh` runs the image with the scorer's flags.

Those three scripts are from the official starter kit. Everything else was written during the event.

License: MIT.
