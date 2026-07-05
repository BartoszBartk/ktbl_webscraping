# Dev Notes — KTBL Webscraper

## Naming convention

**v1 / v2** refer to scraper **code versions**. 
**Run numbers** (1st_run, 2nd_run, 3rd_run …) refer to **data runs** each time the loop is executed .  
These are independent: v2 of the scraper can be used for any number of runs.

Run history:
- Runs before 2026-06-22 used v1 of the scraper (output in parent folder, now archive)
- **3rd_run** → run in June 2026 using v2 scraper; output folder `run_v3_fert/` (legacy name, not renamed because KTBL_preprocessing already reads `ktbl_costs_all_v3_fert.csv`)
- Future runs: set `RUN_TAG <- "4th_run"` (or `"5th_run"` etc.) in `ktbl_multisystem_loop.R` before each run

## 2026-06-22

- Added `cattle_solid_manure` (Rottemist, Rind) to `FERTILIZER_MAP` in `ktbl_multisystem_scraper.r`. Before it was defined in the map but had a silent bug (never wired to the output tibble).

- Added `RUN_TAG` system: each run writes all output to its own `run_<RUN_TAG>/` folder so successive runs never overwrite each other. Change `RUN_TAG` at the top of `ktbl_multisystem_loop.R` before each new run.

- The script produces 48 per-combo CSVs + one combined `ktbl_costs_all_<RUN_TAG>.csv` per run.
