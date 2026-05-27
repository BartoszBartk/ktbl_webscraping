###################################################
# Title: Getting data from KTBL Leistungs-Kosten-Rechner Pflanzenbau (v2)
# Purpose: Loop script that applies the v2 scraper across all parameter
#          combinations. The scraper itself pre-filters invalid combos using
#          ktbl_available_options.csv, so this loop is structurally identical
#          to v1 but consumes the new wider fertilizer schema.
# Author: Bartosz Bartkowski
# Revised by: Giovanna Limon v2
# Input data: ktbl_available_options.csv (produced by
#             ktbl_options_scraper.r)
# Output data: per-combo CSVs + combined ktbl_crops_costs_all.csv
###################################################

# Force UTF-8 locale so German umlauts (ökologisch, etc.) survive round-trips
# through Rscript/httr2 form encoding. Safe to set even if already UTF-8.
Sys.setlocale("LC_ALL", "en_US.UTF-8")

require(httr2)
require(rvest)
require(xml2)
require(dplyr)
require(purrr)
require(stringr)
require(tidyr)
require(readr)
require(here)

# --------------------------
# Output directory: write everything inside webscraping_v2/ so we don't
# overwrite v1 files in the project root.
# --------------------------
OUT_DIR <- here("webscraping_v2")

if (!dir.exists(OUT_DIR)) {
  dir.create(OUT_DIR, recursive = TRUE)
  message("Created output directory: ", OUT_DIR)
}

# --------------------------
# Sanity check: options map must exist before we start the loop.
# We accept either webscraping_v2/ktbl_available_options.csv (preferred) or
# the project-root copy. The scraper reads it from project root via
# here::here(), so make sure that copy exists too.
# --------------------------
if (!file.exists(here("ktbl_available_options.csv"))) {
  stop("ktbl_available_options.csv not found in project root. ",
       "Run ktbl_options_scraper.r first to produce it.")
}

# set link to be passed to the scraping procedure
BASE <- "https://daten.ktbl.de/dslkrpflanze"

# define parameter values to be looped through
cultivation_labels <- c("integriert", "ökologisch")
plot_sizes         <- c(2, 20, 80)
yield_soils        <- c("hoch, mittlerer Boden", "mittel, mittlerer Boden")
mechanizations     <- c(120, 230)
distances          <- c(2, 15)

# create a grid of combinations
combos <- expand.grid(
  v1 = cultivation_labels,
  v2 = plot_sizes,
  v3 = yield_soils,
  v4 = mechanizations,
  v5 = distances,
  KEEP.OUT.ATTRS = FALSE,
  stringsAsFactors = FALSE
)
# corresponding short codes for filenames
combos$labels <- paste0(
  substr(combos$v1, 1, 3), "_",
  combos$v2, "_",
  substr(combos$v3, 1, 4), "_",
  combos$v4, "_",
  combos$v5
)

for (i in seq_len(nrow(combos))) {
  # If this combo's CSV already exists, load it and skip the scrape.
  # This makes the loop RESUMABLE: if a network error kills the run halfway,
  # re-running the loop continues from where it stopped instead of redoing
  # everything from scratch.
  combo_csv <- file.path(OUT_DIR, paste0("ktbl_all_systems_", combos$labels[i], ".csv"))
  if (file.exists(combo_csv)) {
    message("Skipping combo ", i, "/", nrow(combos),
            " (already saved): ", combos$labels[i])
    results_final <- read.csv(combo_csv, stringsAsFactors = FALSE, encoding = "UTF-8")
    assign(paste0("results_final_", i), results_final)
    next
  }

  TARGET <- list(
    cultivation_label = combos$v1[i],
    plot_size_ha      = combos$v2[i],
    yield_soil        = combos$v3[i],
    mechanization     = combos$v4[i],
    distance_km       = combos$v5[i]
  )

  # run scraping script (this only works on Linux/macOS)
  source(here("webscraping_v2", "ktbl_multisystem_scraper.r"))
  # Windows alternative (necessary due to UTF-8 encoding)
  # eval(parse(here("webscraping_v2", "ktbl_multisystem_scraper.r"), encoding = "UTF-8"))

  # add the parameters used to every row of the result
  results_final$system <- rep(combos$v1[i])
  results_final$size   <- rep(combos$v2[i])
  results_final$soil   <- rep(combos$v3[i])
  results_final$mech   <- rep(combos$v4[i])
  results_final$dist   <- rep(combos$v5[i])

  # save results for this combination INSIDE webscraping_v2/
  assign(paste0("results_final_", i), results_final)
  write.csv(results_final,
            file.path(OUT_DIR, paste0("ktbl_all_systems_", combos$labels[i], ".csv")),
            row.names = FALSE)
  cat(paste0("\nSaved: webscraping_v2/ktbl_all_systems_", combos$labels[i], ".csv ("),
      nrow(results_final), " rows)\n", sep = "")
}

# combine all per-combo results into one table
ktbl_crops_costs <- results_final_1
for (i in 2:nrow(combos)) {
  ktbl_crops_costs <- rbind(ktbl_crops_costs, get(paste0("results_final_", i)))
}

# remove (placeholder Anbausystem)
ktbl_crops_costs <- subset(ktbl_crops_costs, production_system != "[Anbausystem]")

# save combined dataset INSIDE webscraping_v2/ with the _v2 suffix
write.csv(ktbl_crops_costs,
          file.path(OUT_DIR, "ktbl_costs_all_v2.csv"),
          row.names = FALSE)
cat("\nSaved combined dataset: webscraping_v2/ktbl_costs_all_v2.csv (",
    nrow(ktbl_crops_costs), " rows)\n", sep = "")


