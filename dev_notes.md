# Dev Notes — KTBL Webscraper

Note: `v1`/`v2` are scraper code versions; `RUN_TAG` is independent and tracks data runs (e.g. `r3`, `r4`).

## 2026-06-22

- Added `cattle_solid_manure` (Rottemist, Rind) to `FERTILIZER_MAP` in `ktbl_multisystem_scraper.r`. Before it was defined in the map but had a silent bug (never wired to the output tibble).

- Added `RUN_TAG` system: each run writes all output to its own `run_<RUN_TAG>/` folder so successive runs never overwrite each other. Change `RUN_TAG` at the top of `ktbl_multisystem_loop.R` before each new run.

- The script produces 48 per-combo CSVs + one combined `ktbl_costs_all_<RUN_TAG>.csv` per run.
