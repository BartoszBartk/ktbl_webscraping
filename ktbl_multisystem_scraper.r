###################################################
# Title: Getting data from KTBL Leistungs-Kosten-Rechner Pflanzenbau (v2)
# Purpose: Webscrape data from KTBL's Leistungs-Kosten-Rechner Pflanzenbau.
#          v2 adds:
#            1) Pre-filter validation: before requesting a (crop, system) with a
#               given TARGET, check `ktbl_available_options.csv` and skip if any
#               of TARGET's parameters (soil, size, mech, dist) is not actually
#               offered by the site for that (crop, system). This fixes the
#               fallback bug where the server silently returned data from a
#               default combination when the requested one didn't exist.
#            2) Fertilizer mapping by German display name into 5 separate
#               English-named columns:
#                 - can_27n            (Kalkammonsalpeter 27 % N)
#                 - dap_18n            (Diammonphosphat 18 % N, 46 % P2O5)
#                 - cattle_slurry      (Gülle, Rind)
#                 - cattle_pig_slurry  (Gülle, Rind und Schwein gemischt)
#                 - biogas_digestate   (Gärrest, Biogasanlage)
#                 - cattle_solid_manure = "Rottemist, Rind"          
#               Uses exact-match
# Author: Bartosz Bartkowski
# Revised by: Giovanna Limon v2
# Input data: ktbl_available_options.csv (produced by ktbl_options_scraper.r)
# Output data: tibble `results_final` with one row per (crop, production_system)
###################################################

######################################
# Stuff that is not needed if this script is called by ktbl_multisystem_loop.R
# library(httr2)
# library(rvest)
# library(xml2)
# library(dplyr)
# library(purrr)
# library(stringr)
# library(tidyr)
# library(readr)
# require(here)

# --------------------------
# Config
# --------------------------
# BASE <- "https://daten.ktbl.de/dslkrpflanze"

# TARGET <- list(
#   cultivation_label = "integriert",
#   plot_size_ha      = 2,
#   yield_soil        = "hoch, mittlerer Boden",
#   mechanization     = 120,
#   distance_km       = 2
# )
#####################################

# For Kulturgruppe selection (can be pre-set by a wrapper like smoke_test.R)
if (!exists("KULTURGRUPPEN")) KULTURGRUPPEN <- c("1", "2", "3", "4", "5", "9") #, "11")
# Available values:
# 1 = Getreide
# 2 = Mais
# 3 = Kartoffeln und Zuckerrüben
# 4 = Futterbau
# 5 = Zwischenfrüchte
# 9 = Ölfrüchte und Eiweißpflanzen
# 11 = Energiepflanzen
# Or use "Alle" for all groups

POLITE_DELAY_MS <- 400

# --------------------------
# Fertilizer mapping: English variable name -> German display label in KTBL
# Used for exact-label extraction below.
# --------------------------
FERTILIZER_MAP <- list(
  can_27n           = "Kalkammonsalpeter (27 % N), lose",
  dap_18n           = "Diammonphosphat (18 % N, 46 % P₂O₅), lose",
  cattle_slurry     = "Gülle, Rind",
  cattle_pig_slurry = "Gülle, Rind und Schwein gemischt",
  biogas_digestate  = "Gärrest, Biogasanlage",
  cattle_solid_manure = "Rottemist, Rind"           # added
)

# --------------------------
# Load available-options reference table (produced by ktbl_options_scraper.r).
# Used to pre-filter invalid (crop, system, parameter) combinations before
# sending them to the server (which would otherwise silently fall back).
# --------------------------
if (!exists("AVAILABLE_OPTIONS")) {
  options_path <- here::here("ktbl_available_options.csv")
  if (!file.exists(options_path)) {
    stop("ktbl_available_options.csv not found. Run ktbl_options_scraper.r first.")
  }
  AVAILABLE_OPTIONS <- read.csv(options_path, stringsAsFactors = FALSE, encoding = "UTF-8")
  # Drop placeholder rows (labels like "[Schlaggröße]")
  AVAILABLE_OPTIONS <- AVAILABLE_OPTIONS[!grepl("^\\[", AVAILABLE_OPTIONS$available_label), ]
}

# --------------------------
# Cookie file + helpers
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

# In case of timeout 
# Returns NULL on failure so callers can handle it and continue the run
# instead of crashing the entire loop.
safe_post_state_html <- function(body, context = "") {
  tryCatch(
    post_state_html(body),
    error = function(e) {
      message("HTTP error", if (nzchar(context)) paste0(" (", context, ")") else "",
              ": ", conditionMessage(e))
      NULL
    }
  )
}

# --------------------------
# DOM utilities
# --------------------------
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

get_all_systems <- function(doc) {
  sel <- html_node(doc, xpath = "//select[@name='cropSysId']")
  if (is.na(html_name(sel))) return(NULL)

  opts <- html_nodes(sel, "option")
  vals <- html_attr(opts, "value")
  labs <- html_text(opts, trim = TRUE)

  # Skip placeholder rows whose label starts with '[' (e.g. "[Anbausystem]")
  valid_idx <- which(!is.na(vals) & nzchar(vals) & !str_starts(labs, fixed("[")))
  if (length(valid_idx) == 0) return(NULL)

  tibble(
    system_value = vals[valid_idx],
    system_label = labs[valid_idx]
  )
}

# NA-row builder with the new fertilizer schema (5 separate columns)
na_result_row <- function(crop_name, system_label) {
  tibble(
    crop = crop_name, production_system = system_label,
    yield = NA_real_, price = NA_real_, turnover = NA_real_,
    direct_costs = NA_real_, direct_cost_free = NA_real_,
    variable_costs = NA_real_, contribution_margin = NA_real_,
    execution_costs = NA_real_,
    can_27n = NA_real_, dap_18n = NA_real_,
    cattle_slurry = NA_real_, cattle_pig_slurry = NA_real_,
    biogas_digestate = NA_real_, cattle_solid_manure = NA_real_,
    income = NA_real_
  )
}

page_matches_crop <- function(doc, expected_crop_val) {
  selected_val <- html_attr(
    html_node(doc, xpath = "//select[@name='cropId']/option[@selected]"),
    "value"
  )
  !is.na(selected_val) && selected_val == expected_crop_val
}

# Convert German number format to R numeric
de_num <- function(x) {
  if (is.na(x) || length(x) == 0) return(NA_real_)
  x <- str_replace_all(x, " ", "")
  x <- str_replace_all(x, " ", "")
  x <- str_replace_all(x, "\\.", "")
  x <- str_replace(x, ",", ".")
  as.numeric(str_extract(x, "-?\\d+(?:\\.\\d+)?"))
}

# --------------------------
# Pre-filter helper: is this (cultivation, crop, system) able to accept
# the requested value for `parameter`?
# --------------------------
is_valid_option <- function(cultivation, crop, prod_system, parameter, value) {
  any(
    AVAILABLE_OPTIONS$cultivation       == cultivation &
    AVAILABLE_OPTIONS$crop              == crop &
    AVAILABLE_OPTIONS$production_system == prod_system &
    AVAILABLE_OPTIONS$parameter         == parameter &
    AVAILABLE_OPTIONS$available_label   == as.character(value)
  )
}

# Returns TRUE iff TARGET's 4 specification parameters are all valid for this
# (cultivation, crop, system) combo according to the options map.
target_is_valid <- function(cultivation, crop, prod_system, target) {
  is_valid_option(cultivation, crop, prod_system, "areaSize",    target$plot_size_ha) &&
  is_valid_option(cultivation, crop, prod_system, "soilHarvest", target$yield_soil) &&
  is_valid_option(cultivation, crop, prod_system, "mechanics",   target$mechanization) &&
  is_valid_option(cultivation, crop, prod_system, "distance",    target$distance_km)
}

# --------------------------
# Results-table parser (with new fertilizer schema)
# --------------------------
parse_results_table <- function(doc, crop_name, system_label) {
  tabs1 <- html_node(doc, css = "#tabs-1")
  if (is.na(html_name(tabs1))) {
    message("  No #tabs-1 found for: ", crop_name, " (", system_label, ")")
    return(NULL)
  }
  table <- html_node(tabs1, "table")
  if (is.na(html_name(table))) {
    message("  No table found for: ", crop_name, " (", system_label, ")")
    return(NULL)
  }

  # Extract yield and price from first data row
  first_data_row <- html_node(table, xpath = ".//tr[td[@class='tabelleEbene2 left']][1]")
  yield_val <- NA_real_
  price_val <- NA_real_

  if (!is.na(html_name(first_data_row))) {
    tds <- html_nodes(first_data_row, "td")
    if (length(tds) >= 5) {
      yield_text <- html_text(tds[2], trim = TRUE)
      yield_val <- de_num(yield_text)

      price_text <- html_text(tds[4], trim = TRUE)
      price_val <- de_num(price_text)
    }
  }

  # Find a "summary value" by exact label (financial figures, second-to-last cell)
  find_value_by_label <- function(label_pattern) {
    target <- stringr::str_to_lower(stringr::str_trim(label_pattern))

    xpath_query <- sprintf(
      ".//tr[td[contains(@class,'tabelle') and
              translate(normalize-space(translate(., ':' , '')),
                        'ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ',
                        'abcdefghijklmnopqrstuvwxyzäöü'
              ) = '%s'
         ]]",
      target
    )

    matching_rows <- rvest::html_nodes(table, xpath = xpath_query)

    for (row in matching_rows) {
      tds <- rvest::html_nodes(row, "td")
      if (length(tds) >= 2) {
        val_td <- tds[length(tds) - 1]
        val_text <- rvest::html_text(val_td, trim = TRUE)
        if (nzchar(val_text)) {
          return(de_num(val_text))
        }
      }
    }
    return(NA_real_)
  }

  # Find an "amount" value by EXACT label (input rows like fertilizers,
  # second cell). Uses exact match instead of starts-with to avoid the
  # "Gülle, Rind" / "Gülle, Rind und Schwein gemischt" collision.
  find_amount_by_exact_label <- function(label_pattern) {
    target <- stringr::str_to_lower(stringr::str_trim(label_pattern))

    xpath_query <- sprintf(
      ".//tr[td[contains(@class,'tabelle') and
              translate(normalize-space(translate(., ':' , '')),
                        'ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ',
                        'abcdefghijklmnopqrstuvwxyzäöü'
              ) = '%s'
         ]]",
      target
    )

    matching_rows <- rvest::html_nodes(table, xpath = xpath_query)

    for (row in matching_rows) {
      tds <- rvest::html_nodes(row, "td")
      if (length(tds) >= 2) {
        val_td <- tds[2]
        val_text <- rvest::html_text(val_td, trim = TRUE)
        if (nzchar(val_text)) {
          return(de_num(val_text))
        }
      }
    }
    return(NA_real_)
  }

  # Extract key financial metrics
  turnover            <- find_value_by_label("summe leistung")
  direct_costs        <- find_value_by_label("summe direktkosten")
  direct_cost_free    <- find_value_by_label("direktkostenfreie leistung")
  variable_costs      <- find_value_by_label("summe variable kosten")
  contribution_margin <- find_value_by_label("deckungsbeitrag")
  execution_costs     <- find_value_by_label("arbeitserledigungskosten")

  # Extract fertilizer amounts using the German-name -> English-name mapping
  fert_values <- lapply(FERTILIZER_MAP, find_amount_by_exact_label)

  # Compute income strictly from the three components. If any of them is NA,
  # leave income = NA so the parsing failure stays visible
  income <- if (!is.na(turnover) && !is.na(direct_costs) && !is.na(execution_costs)) {
    turnover - direct_costs - execution_costs
  } else {
    NA_real_
  }

  message("  [SUCCESS] ", crop_name, " | ", system_label,
          " - yield: ", round(yield_val, 2), " t/ha",
          ", price: ", round(price_val, 2), " €/t",
          ", turnover: ", round(turnover, 2),
          ", direct_costs: ", round(direct_costs, 2),
          ", variable_costs: ", round(variable_costs, 2),
          ", CAN: ", round(fert_values$can_27n, 2),
          ", DAP: ", round(fert_values$dap_18n, 2),
          ", cattle_slurry: ", round(fert_values$cattle_slurry, 2),
          ", cattle_pig_slurry: ", round(fert_values$cattle_pig_slurry, 2),
          ", biogas_digestate: ", round(fert_values$biogas_digestate, 2),
          ", cattle_solid_manure: ", round(fert_values$cattle_solid_manure, 2),
          ", contribution_margin: ", round(contribution_margin, 2))

  tibble(
    crop = crop_name,
    production_system = system_label,
    yield = yield_val,
    price = price_val,
    turnover = turnover,
    direct_costs = direct_costs,
    direct_cost_free = direct_cost_free,
    variable_costs = variable_costs,
    contribution_margin = contribution_margin,
    execution_costs = execution_costs,
    can_27n = fert_values$can_27n,
    dap_18n = fert_values$dap_18n,
    cattle_slurry = fert_values$cattle_slurry,
    cattle_pig_slurry = fert_values$cattle_pig_slurry,
    biogas_digestate = fert_values$biogas_digestate,
    cattle_solid_manure = fert_values$cattle_solid_manure,
    income = income
  )
}

# --------------------------
# Main flow
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

# 2) state=1 -> Wirtschaftsart
cultivation_val <- option_value_by_label(doc, "cultivation", TARGET$cultivation_label)
if (is.na(cultivation_val)) cultivation_val <- TARGET$cultivation_label
doc <- post_state_html(list(state = 1, cultivation = cultivation_val))

# 3) Read crop list
crop_sel <- html_node(doc, xpath = "//select[@name='cropId']")
if (is.na(html_name(crop_sel))) stop("Could not find crop select (cropId).")

crops <- tibble(
  crop = html_text(html_nodes(crop_sel, "option"), trim = TRUE),
  value = html_attr(html_nodes(crop_sel, "option"), "value")
) |>
  filter(!is.na(value), nzchar(value))

message("Found ", nrow(crops), " crops.")

# 4) Loop through crops and all their available systems
results <- map_dfr(seq_len(nrow(crops)), function(i) {
  crop_name <- crops$crop[i]
  crop_val  <- crops$value[i]

  Sys.sleep(POLITE_DELAY_MS / 1000)

  # state=2 -> choose crop (safe: skip this crop if the network call fails)
  doc2 <- safe_post_state_html(list(state = 2, cropId = crop_val), crop_name)
  if (is.null(doc2)) {
    message("Skip (network error choosing crop): ", crop_name)
    return(NULL)
  }

  # Get all available systems for this crop
  systems <- get_all_systems(doc2)

  if (is.null(systems) || nrow(systems) == 0) {
    message("Skip (no systems available): ", crop_name)
    return(NULL)
  }

  message("Processing ", crop_name, " with ", nrow(systems), " production system(s)...")

  # Loop through all available systems for this crop
  crop_results <- map_dfr(seq_len(nrow(systems)), function(j) {
    sys_val <- systems$system_value[j]
    sys_label <- systems$system_label[j]

    # --- Pre-filter: skip combos KTBL does not actually offer ---
    if (!target_is_valid(TARGET$cultivation_label, crop_name, sys_label, TARGET)) {
      message("  Pre-filter skip: ", crop_name, " | ", sys_label,
              " - TARGET params not all valid for this combo")
      return(NULL)
    }

    if (j > 1) Sys.sleep(POLITE_DELAY_MS / 1000)

    ctx <- paste0(crop_name, " | ", sys_label)

    # Each state transition is wrapped in safe_post_state_html so a single
    # network timeout does not crash the whole loop. If any step fails we
    # emit an NA row and continue with the next (crop, system).
    doc3 <- safe_post_state_html(list(state = 3, cropSysId = sys_val), ctx)
    if (is.null(doc3)) return(na_result_row(crop_name, sys_label))

    doc_area <- safe_post_state_html(list(
      state = 11, areaSize = TARGET$plot_size_ha, refineSelection = "true"), ctx)
    if (is.null(doc_area)) return(na_result_row(crop_name, sys_label))

    doc_yield <- safe_post_state_html(list(
      state = 12, soilHarvest = TARGET$yield_soil, refineSelection = "true"), ctx)
    if (is.null(doc_yield)) return(na_result_row(crop_name, sys_label))

    doc_mech <- safe_post_state_html(list(
      state = 5, mechanics = TARGET$mechanization, refineSelection = "true"), ctx)
    if (is.null(doc_mech)) return(na_result_row(crop_name, sys_label))

    doc_dist <- safe_post_state_html(list(
      state = 5, distance = TARGET$distance_km, refineSelection = "true"), ctx)
    if (is.null(doc_dist)) return(na_result_row(crop_name, sys_label))

    doc8 <- safe_post_state_html(list(state = 8), ctx)
    if (is.null(doc8)) return(na_result_row(crop_name, sys_label))

    if (!page_matches_crop(doc8, crop_name)) {
      selected_val <- html_attr(
        html_node(doc8, xpath = "//select[@name='cropId']/option[@selected]"),
        "value"
      )
      message("Session state mismatch: expected '", crop_name, "' (", sys_label, ")",
              " but page has selected cropId: '", selected_val, "' — returning NA row")
      return(na_result_row(crop_name, sys_label))
    }

    out <- parse_results_table(doc8, crop_name, sys_label)
    if (is.null(out)) {
      message("No table parsed for: ", crop_name, " (", sys_label, ")")
      return(na_result_row(crop_name, sys_label))
    }
    out
  })

  crop_results
})

# 5) Format and save results
# arrange(is.na(income)) before distinct() ensures NA rows go AFTER real rows,
# so if duplicates ever exist for the same (crop, system), distinct() keeps
# the row that actually has data instead of the NA placeholder.
results_final <- results |>
  arrange(crop, production_system, is.na(income)) |>
  distinct(crop, production_system, .keep_all = TRUE) |>
  select(crop, production_system, yield, price, turnover, direct_costs,
         direct_cost_free, variable_costs, contribution_margin,
         execution_costs,
         can_27n, dap_18n, cattle_slurry, cattle_pig_slurry, biogas_digestate,
         cattle_solid_manure,
         income) |>
  arrange(crop, production_system)

# Clean up
unlink(cookie_file)
