###################################################
# Title: Getting data from KTBL Leistungs-Kosten-Rechner Pflanzenbau
# Purpose: A loop script to get data from KTBL Leistungs-Kosten-Rechner Pflanzenbau for different parameter combinations
# Author: Bartosz Bartkowski
# Input data: none
# Output data: dataset with yields, prices and costs for different crops and management combinations
###################################################
require(httr2)
require(rvest)
require(xml2)
require(dplyr)
require(purrr)
require(stringr)
require(tidyr)
require(readr)
require(here)

# define loop to apply Christoph Pahmeyer's modified KTBL Leistungs-Kosten-Rechner Pflanzenbau to multiple parameter combinations
# first, set link to be passed to the scraping procedure
BASE <- "https://daten.ktbl.de/dslkrpflanze"

# define parameter values to be looped through
cultivation_labels <- c("integriert", "ökologisch")
plot_sizes <- c(2, 20, 80)
yield_soils <- c("hoch, mittlerer Boden", "mittel, mittlerer Boden")
mechanizations <- c(120, 230)
distances <- c(2, 15)

# create a grid of combinations
combos <- expand.grid(v1 = cultivation_labels, v2 = plot_sizes, v3 = yield_soils, v4 = mechanizations, v5 = distances, 
                      KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
# …add column with corresponding codes to be used in file names
combos$labels <- paste0(substr(combos$v1, 1, 3), "_", combos$v2, "_", substr(combos$v3, 1, 4), "_", combos$v4, "_", combos$v5)

for (i in seq_len(nrow(combos))) {
  # list of parameters
  TARGET <- list(
    cultivation_label = combos$v1[i],
    plot_size_ha      = combos$v2[i],
    yield_soil        = combos$v3[i],
    mechanization     = combos$v4[i],
    distance_km       = combos$v5[i]
  )
  
  # run scraping script (this only works on Linux)
  source(here("ktbl_multisystem_scraper.r"))
  # Windows alternative (necessary due to the UTF-8 encoding)
  # eval(parse(here("ktbl_multisystem_scraper.r"), encoding="UTF-8"))
  
  # add parameters to dataframe
  results_final$system <- rep(combos$v1[i])
  results_final$size <- rep(combos$v2[i])
  results_final$soil <- rep(combos$v3[i])
  results_final$mech <- rep(combos$v4[i])
  results_final$dist <- rep(combos$v5[i])
  
  # save results
  assign(paste0("results_final_", i), results_final)
  write.csv(results_final, here(paste0("ktbl_all_systems_", combos$labels[i], ".csv")))
  cat(paste0("\nSaved: ktbl_all_systems_", combos$labels[i], ".csv ("), nrow(results_final), " rows)\n", sep = "")
}

# create one big-ass table with all data
ktbl_crops_costs <- results_final_1
for (i in 2:nrow(combos)) {
  ktbl_crops_costs <- rbind(ktbl_crops_costs, get(paste0("results_final_", i)))
}

# clean that table by removing artifact rows
ktbl_crops_costs <- subset(ktbl_crops_costs, production_system != "[Anbausystem]")

# save that monster to file
write.csv(ktbl_crops_costs, here("ktbl_crops_costs_all.csv"))
