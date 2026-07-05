# Dev Notes KTBL Webscraper (webscraping_v3)

## 2026-06-22

- Added `cattle_solid_manure` (Rottemist, Rind) to `FERTILIZER_MAP` in `ktbl_multisystem_scraper.r` . 
  Before it was defined but had a silence bug. 
  
- Add `RUN_TAG`, so if new runs are done each run writes all output on its own folder, to avoid mixing previous runs, this has to change before new run. Right now it is named ("v3_fert") 

- The script produces 48 per-combo CSVs and `ktbl_crops_costs_all.csv` (now v3)
