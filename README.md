# Webscraping data from KTBL database — v2

This repository contains scripts to extract data from [KTBL's Leistungs-Kosten-Rechner Pflanzenbau](https://daten.ktbl.de/dslkrpflanze) on crop yields, prices and site- and management-specific costs. 
The scripts were developed under the [Agriscape project at UFZ](https://www.ufz.de/agriscape/)

## Files

- `ktbl_options_scraper.r` — runs **once** to map which parameter values KTBL offers for each (crop × production system). Produces `ktbl_available_options.csv`. Re-run only if KTBL is updated or resctructured. 
- `ktbl_multisystem_scraper.r` — the main scraper for a single targetcombination. Uses the options map (options scraper output) to prefilter invalid combinations before sending request. 
- `ktbl_multisystem_loop.R` — runs the scraper across all (n=48) parameter combinations and writes the combined dataset to `ktbl_crops_costs_all.csv`.  
- `archive/fertilizer_scraper.r` — exploratory script kept for reference of the existing fertilizers across the different combinations. 

This is a second version **v2** of the scraper, with a series of fixes documented below, **v1** can be found as an archive branch. 

## Output data structure 

One row per (crop × production_system × parameter combination) ~6,500 rows × 21 columns.

| `crop` | `production_system` | `yield` | `price` | `turnover` | `direct_costs` | `direct_cost_free` | `variable_costs` | `contribution_margin` | `execution_costs` | `can_27n` |
|---|---|---|---|---|---|---|---|---|---|---|
| — | — | t/ha | €/t | €/ha | €/ha | €/ha | €/ha | €/ha | €/ha | kg/ha |

| `dap_18n` | `cattle_slurry` | `cattle_pig_slurry` | `biogas_digestate` | `cattle_solid_manure` | `income` | `system` | `size` | `soil` | `mech` | `dist` |
|---|---|---|---|---|---|---|---|---|---|---|
| kg/ha | m³/ha | m³/ha | m³/ha | t/ha | €/ha | — | ha | — | kW | km |

## What v2 fixes

### 1. Fallback bug (main fix)
In v1, when the scraper requested a parameter combination KTBL didn't actually offer (e.g. `hoch, mittlerer Boden` for an organic crop that only supports `mittel`), the server silently returned data from a different combination. The scraper then labelled those results with the requested parameters — producing **incorrect rows** (error was mostly in `soil` and `size`)

**v2 fix**: before sending any request, the scraper checks `ktbl_available_options.csv` and skips combinations KTBL doesn't offer. Result: ~6,500 rows instead of ~8,100 with errors.

### 2. Fertilizer schema rewritte
v1 collapsed all nitrogen and organic fertilizers into two columns (`n_fertilizer`, `org_fertilizer`) using prefix matching.
In **v2**: 5 separate columns with labeling, raw amounts in kg/ha or m³/ha:

| Variable | Name | N content |
|---|---|---|
| `can_27n` | Kalkammonsalpeter (27 % N), lose | 27 % |
| `dap_18n` | Diammonphosphat (18 % N, 46 % P₂O₅), lose | 18 % |
| `cattle_slurry` | Gülle, Rind | ~3.5–4.5 kg N/m³ |
| `cattle_pig_slurry` | Gülle, Rind und Schwein gemischt | ~4–5 kg N/m³ |
| `biogas_digestate` | Gärrest, Biogasanlage | ~5–6 kg N/m³ |
| `cattle_solid_manure` | Rottemist, Rind | ~6.11 kg N/t |

### 3. Other fixes
- v1 `income` calculation fell back to `contribution_margin` when components were missing, the fallback didn't fire in the v1; however v2 removes this fallback system in case of `NA`.
- UTF-8 locale forced at the start of the loop (umlaut's issues).
- `[Anbausystem]` placeholder filtered at source. 
- `distinct()` arranged so non-NA rows are preferred when duplicates exist.
- Outputs written to `webscraping_v2/` so v1 files in the parent stay intact.

## Run the scraper and the loop

Before running, set `RUN_TAG` at the top of `ktbl_multisystem_loop.R`. Each run writes all output to its own folder `run_<RUN_TAG>/` so previous runs are never overwritten. Use `Nth_run` naming (e.g. `"4th_run"`) — `v1`/`v2` are scraper code versions, not run numbers.

```r
# 0. Set the run tag in ktbl_multisystem_loop.R before running:
#    RUN_TAG <- "4th_run"  # increment for each new run; v1/v2 are scraper versions, not run numbers

# 1. (Only if KTBL options have changed) re-build the options map
source("ktbl_options_scraper.r")   # ~1 hour

# 2. (Optional) Save all run messages to a log file for later inspection
log_con <- file(paste0("run_", RUN_TAG, "/loop_run.log"), open = "wt")
sink(log_con, type = "message")

# 3. Run the full loop
source("ktbl_multisystem_loop.R")  # ~5-7 hours

# 4. Close the log
sink(type = "message")
close(log_con)
```

Output: `run_<RUN_TAG>/ktbl_costs_all_<RUN_TAG>.csv` (combined) + 48 per-combo CSVs in the same folder.

## Limitations

- For some crops with multi-component yield (e.g. soy, grass with multiple cuts), only the first yield component is scraped. As a result, `yield × price` may not equal `turnover` for those rows. The `turnover` and downstream financial figures remain correct.
- Other products beyond the 6 mapped fertilizers  (e.g. Hühnermist) are not included. 

## Authors

Original (v1): Bartosz Bartkowski (bartosz.bartkowski@ufz.de) and Malin Gütschow (malin-sophie.guetschow@ufz.de), based on [Christoph Pahmeyer's approach](https://github.com/fruchtfolge/KTBL-APIs).

V2: Giovanna Limon (giovanna.limon@ufz.de), May 2026.
The v2 (pre-filter design and code review) was developed with assistance from [Anthropic's Claude Code (Sonnet 4.5)](https://www.anthropic.com/claude)
