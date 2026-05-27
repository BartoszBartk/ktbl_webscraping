###################################################
# Title: KTBL Leistungs-Kostenrechner Pflanzenbau - Crop/System Options Mapper
# Purpose: For every (crop x production_system), record which values the KTBL site
#          offers in the 4 specification dropdowns (areaSize, soilHarvest,
#          mechanics, distance). This produces a reference table of valid
#          parameter combinations. 
# Author: Giovanna Limon based on Bartosz Bartkowski's scraper
# Input data: none
# Output data: long-format dataset with one row per (crop, production_system,
#              parameter, available_value, available_label)
###################################################

######################################
# Stuff that is not needed if this script is called by a loop wrapper
library(httr2)
library(rvest)
library(xml2)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(readr)
library(here)

# --------------------------
# Config
# --------------------------
BASE <- "https://daten.ktbl.de/dslkrpflanze"

# Which cultivation types to map. By default both.
CULTIVATIONS <- c("integriert", "ökologisch")

# For Kulturgruppe selection (same as main scraper)
KULTURGRUPPEN <- c("1", "2", "3", "4", "5", "9")

POLITE_DELAY_MS <- 400

# The 4 specification dropdowns we want to map
SPEC_DROPDOWNS <- c("areaSize", "soilHarvest", "mechanics", "distance")

# --------------------------
# Cookie file + helpers (reused from main scraper)
# --------------------------
cookie_file <- tempfile(fileext = ".rds")

make_req <- function(url) {
  request(url) |>
    req_cookie_preserve(cookie_file) |>
    req_user_agent("KTBL-Scraper (R httr2)") |>
    req_timeout(30) |>
    req_retry(max_tries = 3)
}

resp_html <- function(resp) {
  read_html(resp_body_string(resp))
}

post_state_html <- function(body) {
  make_req(file.path(BASE, "postHv.html")) |>
    req_headers(`Content-Type` = "application/x-www-form-urlencoded") |>
    req_body_form(!!!body) |>
    req_perform() |>
    resp_html()
}

option_value_by_label <- function(doc, select_name, label_text) {
  sel <- html_node(doc, xpath = sprintf("//select[@name='%s']", select_name))
  if (is.na(html_name(sel))) return(NA_character_)
  opts <- html_nodes(sel, "option")
  labs <- html_text(opts, trim = TRUE)
  vals <- html_attr(opts, "value")
  idx  <- which(str_to_lower(labs) == str_to_lower(label_text))
  if (length(idx) == 0) idx <- which(str_detect(str_to_lower(labs), str_to_lower(label_text)))
  if (length(idx) == 0) return(NA_character_)
  vals[idx[1]]
}

# Get all available system values and labels for a crop
get_all_systems <- function(doc) {
  sel <- html_node(doc, xpath = "//select[@name='cropSysId']")
  if (is.na(html_name(sel))) return(NULL)

  opts <- html_nodes(sel, "option")
  vals <- html_attr(opts, "value")
  labs <- html_text(opts, trim = TRUE)

  valid_idx <- which(!is.na(vals) & nzchar(vals))
  if (length(valid_idx) == 0) return(NULL)

  tibble(
    system_value = vals[valid_idx],
    system_label = labs[valid_idx]
  )
}

# --------------------------
# Core: read all options offered by a specific dropdown on the current page
# --------------------------
get_dropdown_options <- function(doc, select_name) {
  sel <- html_node(doc, xpath = sprintf("//select[@name='%s']", select_name))
  if (is.na(html_name(sel))) return(NULL)

  opts <- html_nodes(sel, "option")
  vals <- html_attr(opts, "value")
  labs <- html_text(opts, trim = TRUE)

  # Keep only options with a non-empty value
  valid_idx <- which(!is.na(vals) & nzchar(vals))
  if (length(valid_idx) == 0) return(NULL)

  tibble(
    available_value = vals[valid_idx],
    available_label = labs[valid_idx]
  )
}

# --------------------------
# Main flow: for every (cultivation, crop, system), record the offered options
# in each of the 4 specification dropdowns
# --------------------------

# 0) Initial GET to seed cookies
make_req("http://daten.ktbl.de/dslkrpflanze/?tx_ktblsso_checktoken[token]=") |>
  req_perform() -> resp0

# 1) state=10 -> Kulturgruppen selection
if (identical(KULTURGRUPPEN, "Alle")) {
  doc <- post_state_html(list(state = 10, selectedKulturgruppen = "Alle"))
} else {
  kulturgruppen_params <- list(state = 10)
  for (kg in KULTURGRUPPEN) {
    kulturgruppen_params <- c(kulturgruppen_params, list(selectedKulturgruppen = kg))
  }
  doc <- make_req(file.path(BASE, "postHv.html")) |>
    req_headers(`Content-Type` = "application/x-www-form-urlencoded") |>
    req_body_form(!!!kulturgruppen_params) |>
    req_perform() |>
    resp_html()
}

# 2) Loop over cultivation types
all_options <- map_dfr(CULTIVATIONS, function(cult_label) {
  message("\n=== Cultivation: ", cult_label, " ===")

  cult_val <- option_value_by_label(doc, "cultivation", cult_label)
  if (is.na(cult_val)) cult_val <- cult_label
  doc_cult <- post_state_html(list(state = 1, cultivation = cult_val))

  # Read crop list for this cultivation
  crop_sel <- html_node(doc_cult, xpath = "//select[@name='cropId']")
  if (is.na(html_name(crop_sel))) {
    message("Could not find cropId select for cultivation ", cult_label)
    return(NULL)
  }

  crops <- tibble(
    crop = html_text(html_nodes(crop_sel, "option"), trim = TRUE),
    crop_value = html_attr(html_nodes(crop_sel, "option"), "value")
  ) |>
    filter(!is.na(crop_value), nzchar(crop_value))

  message("Found ", nrow(crops), " crops for ", cult_label)

  # For each crop, enumerate systems and read all dropdowns
  map_dfr(seq_len(nrow(crops)), function(i) {
    crop_name <- crops$crop[i]
    crop_val  <- crops$crop_value[i]

    Sys.sleep(POLITE_DELAY_MS / 1000)

    # state=2 -> choose crop
    doc2 <- post_state_html(list(state = 2, cropId = crop_val))

    systems <- get_all_systems(doc2)
    if (is.null(systems) || nrow(systems) == 0) {
      message("Skip (no systems available): ", crop_name)
      return(NULL)
    }

    message("Mapping ", crop_name, " with ", nrow(systems), " system(s)...")

    map_dfr(seq_len(nrow(systems)), function(j) {
      sys_val   <- systems$system_value[j]
      sys_label <- systems$system_label[j]

      if (j > 1) Sys.sleep(POLITE_DELAY_MS / 1000)

      # state=3 -> select system. After this state the spec dropdowns appear.
      doc3 <- post_state_html(list(state = 3, cropSysId = sys_val))

      # For each of the 4 spec dropdowns, read what's available
      map_dfr(SPEC_DROPDOWNS, function(dd) {
        opts <- get_dropdown_options(doc3, dd)
        if (is.null(opts) || nrow(opts) == 0) {
          message("  No options found in dropdown '", dd, "' for ",
                  crop_name, " | ", sys_label)
          return(NULL)
        }

        opts |>
          mutate(
            cultivation = cult_label,
            crop = crop_name,
            production_system = sys_label,
            parameter = dd,
            .before = 1
          )
      })
    })
  })
})

# --------------------------
# Save reference table
# --------------------------
options_final <- all_options |>
  arrange(cultivation, crop, production_system, parameter, available_value)

write.csv(options_final,
          here("ktbl_available_options.csv"),
          row.names = FALSE,
          fileEncoding = "UTF-8")

cat("\n=== Done. Saved ", nrow(options_final),
    " rows to ktbl_available_options.csv ===\n", sep = "")

# --------------------------
# Quick summary
# --------------------------
cat("\n=== Number of available values per (crop, system, parameter) ===\n")
options_final |>
  count(cultivation, crop, production_system, parameter, name = "n_options") |>
  head(20) |>
  print()

# Clean up
unlink(cookie_file)