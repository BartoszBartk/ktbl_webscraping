###################################################
# Title: Getting data from KTBL Leistungs-Kosten-Rechner Pflanzenbau
# Purpose: A script based on Christoph Pahmeyer's approach (https://github.com/fruchtfolge/KTBL-APIs) to webscrape data from KTBL's Leistungs-Kosten-Rechner Pflanzenbau
# Author: Bartosz Bartkowski
# Input data: none
# Output data: dataset with yields, prices and costs for different crops and management combinations
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

# list of parameters including all variants to go through (not used when combining with the loop in ktbl_multisystem_loop.R)
# TARGET <- list(
#   cultivation_label = "integriert",
#   # cultivation_label = "ökologisch"
#   plot_size_ha      = 2,
#   # plot_size_ha     = 20,
#   # plot_size_ha     = 80,
#   yield_soil        = "hoch, mittlerer Boden",
#   # yield_soil       = "mittel, mittlerer Boden",
#   mechanization     = 120,
#   # mechanization    = 230,
#   distance_km       = 2
#   # distance_km      = 15
# )
#####################################

# For Kulturgruppe selection
KULTURGRUPPEN <- c("1", "2", "3", "4", "5", "9")#, "11")
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

# Get all available system values and labels for a crop
get_all_systems <- function(doc) {
  sel <- html_node(doc, xpath = "//select[@name='cropSysId']")
  if (is.na(html_name(sel))) return(NULL)
  
  opts <- html_nodes(sel, "option")
  vals <- html_attr(opts, "value")
  labs <- html_text(opts, trim = TRUE)
  
  # Filter out empty values
  valid_idx <- which(!is.na(vals) & nzchar(vals))
  
  if (length(valid_idx) == 0) return(NULL)
  
  tibble(
    system_value = vals[valid_idx],
    system_label = labs[valid_idx]
  )
}

# Convert German number format to R numeric
de_num <- function(x) {
  if (is.na(x) || length(x) == 0) return(NA_real_)
  x <- str_replace_all(x, "\u00A0", "")
  x <- str_replace_all(x, " ", "")
  x <- str_replace_all(x, "\\.", "")
  x <- str_replace(x, ",", ".")
  as.numeric(str_extract(x, "-?\\d+(?:\\.\\d+)?"))
}

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
  
  # Function to find value in a row by label text
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
  
  # similar function specifically for more complex data (e.g. fertilizer amount)
  find_amount_by_label <- function(label_pattern) {
    target <- stringr::str_to_lower(stringr::str_trim(label_pattern))
    
    xpath_query <- sprintf(
      ".//tr[
        td[contains(@class,'tabelle') and
           starts-with(
             translate(
               normalize-space(translate(., ':' , '')),
               'ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ',
               'abcdefghijklmnopqrstuvwxyzäöü'
             ),
             '%s')
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
  turnover <- find_value_by_label("summe leistung")
  direct_costs <- find_value_by_label("summe direktkosten")
  direct_cost_free <- find_value_by_label("direktkostenfreie leistung")
  variable_costs <- find_value_by_label("summe variable kosten")
  contribution_margin <- find_value_by_label("deckungsbeitrag")
  execution_costs <- find_value_by_label("arbeitserledigungskosten")
  n_fertilizer <- find_amount_by_label("kalkammonsalpeter") # 27 % N
  org_fertilizer <- find_amount_by_label("gülle, rind")

  # Calculate income
  income <- if (!is.na(turnover) && !is.na(direct_costs) && !is.na(execution_costs)) {
    turnover - direct_costs - execution_costs
  } else {
    contribution_margin
  }
  
  message("  [SUCCESS] ", crop_name, " | ", system_label,
          " - yield: ", round(yield_val, 2), " t/ha",
          ", price: ", round(price_val, 2), " €/t",
          ", turnover: ", round(turnover, 2), 
          ", direct_costs: ", round(direct_costs, 2),
          ", variable_costs: ", round(variable_costs, 2),
          ", fertilizer amount: ", round(n_fertilizer, 2),
          ", organic fertilizer amount: ", round(org_fertilizer, 2),
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
    n_fertilizer = n_fertilizer,
    org_fertilizer = org_fertilizer,
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

# 2) state=1 -> Wirtschaftsart = integriert
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
  
  # state=2 -> choose crop
  doc2 <- post_state_html(list(state = 2, cropId = crop_val))
  
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
    
    if (j > 1) Sys.sleep(POLITE_DELAY_MS / 1000)
    
    # Select system (state=3)
    doc3 <- post_state_html(list(state = 3, cropSysId = sys_val))
    
    # Set specifications
    doc_area <- post_state_html(list(
      state = 11,
      areaSize = TARGET$plot_size_ha,
      refineSelection = "true"
    ))
    
    doc_yield <- post_state_html(list(
      state = 12,
      soilHarvest = TARGET$yield_soil,
      refineSelection = "true"
    ))
    
    doc_mech <- post_state_html(list(
      state = 5,
      mechanics = TARGET$mechanization,
      refineSelection = "true"
    ))
    
    doc_dist <- post_state_html(list(
      state = 5,
      distance = TARGET$distance_km,
      refineSelection = "true"
    ))
    
    # state=8 -> calculate
    doc8 <- post_state_html(list(state = 8))
    
    out <- parse_results_table(doc8, crop_name, sys_label)
    if (is.null(out)) {
      message("No table parsed for: ", crop_name, " (", sys_label, ")")
      return(NULL)
    }
    out
  })
  
  crop_results
})

# 5) Format and save results
results_final <- results |>
  distinct(crop, production_system, .keep_all = TRUE) |>
  select(crop, production_system, yield, price, turnover, direct_costs, 
         direct_cost_free, variable_costs, contribution_margin, 
         execution_costs, n_fertilizer, org_fertilizer, income) |>
  arrange(crop, production_system)

# print(results_final, n = 40)

# test-save results
# write.csv(results_final, here("ktbl_all_systems_test.csv"))
# cat("\nSaved: ktbl_all_systems_test.csv (", nrow(results_final), " rows)\n", sep = "")

# Clean up
unlink(cookie_file)