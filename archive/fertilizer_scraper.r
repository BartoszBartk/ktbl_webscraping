###################################################
# Title: KTBL Leistungs-Kostenrechner Pflanzenbau - Fertilizer/Inputs Scraper
# Purpose: Scrape ALL direct cost line items (seeds, fertilizers, lime, interest, etc.)
#          for every crop x production system combination, in long format.
#          Companion to ktbl_multisystem_scraper.r — same navigation, different parser.
# Author: based on Bartosz Bartkowski's scraper
# Input data: none
# Output data: long-format dataset with one row per (crop, production_system, item)
#              containing label, amount, unit, unit price, total cost
###################################################

######################################
# Stuff that is not needed if this script is called by a loop wrapper
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
 BASE <- "https://daten.ktbl.de/dslkrpflanze"

 TARGET <- list(
   cultivation_label = "integriert",
   plot_size_ha      = 2,
   yield_soil        = "hoch, mittlerer Boden",
  mechanization     = 120,
   distance_km       = 2
 )
#####################################

# For Kulturgruppe selection
KULTURGRUPPEN <- c("1", "2", "3", "4", "5", "9")#, "11")

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

  valid_idx <- which(!is.na(vals) & nzchar(vals))
  if (length(valid_idx) == 0) return(NULL)

  tibble(
    system_value = vals[valid_idx],
    system_label = labs[valid_idx]
  )
}

# Build a single NA row to emit when a combination cannot be parsed
na_input_row <- function(crop_name, system_label) {
  tibble(
    crop = crop_name,
    production_system = system_label,
    item_label = NA_character_,
    amount = NA_real_,
    amount_unit = NA_character_,
    unit_price = NA_real_,
    unit_price_unit = NA_character_,
    total_cost = NA_real_
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

# Normalize a label cell for matching (lowercase, trimmed, no colon)
normalize_label <- function(x) {
  x <- str_replace_all(x, ":", "")
  x <- str_squish(x)
  str_to_lower(x)
}

# --------------------------
# Long-format parser:
# Extract every line item in the Direktkosten block (between "Summe Leistung"
# and "Summe Direktkosten") as a long-format tibble.
# --------------------------
parse_input_items <- function(doc, crop_name, system_label) {
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

  rows <- html_nodes(table, "tr")
  if (length(rows) == 0) return(NULL)

  # First-cell text for each row, normalized
  first_cell_text <- vapply(rows, function(r) {
    tds <- html_nodes(r, "td")
    if (length(tds) == 0) return(NA_character_)
    normalize_label(html_text(tds[1], trim = TRUE))
  }, character(1))

  start_match <- which(first_cell_text == "summe leistung")
  end_match   <- which(first_cell_text == "summe direktkosten")

  if (length(start_match) == 0 || length(end_match) == 0) {
    message("  Could not locate Direktkosten block for: ", crop_name, " (", system_label, ")")
    return(NULL)
  }

  start_idx <- start_match[1] + 1
  end_idx   <- end_match[1] - 1
  if (end_idx < start_idx) return(NULL)

  item_rows <- rows[start_idx:end_idx]

  parsed <- purrr::map_dfr(item_rows, function(row) {
    tds <- html_nodes(row, "td")
    if (length(tds) < 2) return(NULL)

    cells <- html_text(tds, trim = TRUE)
    label <- cells[1]

    # Skip empty/structural rows
    if (!nzchar(label)) return(NULL)

    # Defensive extraction: the table layout has the columns
    # [label, amount, amount_unit, unit_price, unit_price_unit, total_cost]
    # but some rows may have fewer cells. We pad with NA.
    get_cell <- function(i) if (length(cells) >= i) cells[i] else NA_character_

    tibble(
      crop = crop_name,
      production_system = system_label,
      item_label = label,
      amount = de_num(get_cell(2)),
      amount_unit = get_cell(3),
      unit_price = de_num(get_cell(4)),
      unit_price_unit = get_cell(5),
      total_cost = de_num(get_cell(6))
    )
  })

  if (nrow(parsed) == 0) return(NULL)

  message("  [SUCCESS] ", crop_name, " | ", system_label,
          " - ", nrow(parsed), " input item(s) extracted")

  parsed
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

# 4) Loop through crops and systems, parsing inputs
inputs_raw <- map_dfr(seq_len(nrow(crops)), function(i) {
  crop_name <- crops$crop[i]
  crop_val  <- crops$value[i]

  Sys.sleep(POLITE_DELAY_MS / 1000)

  # state=2 -> choose crop
  doc2 <- post_state_html(list(state = 2, cropId = crop_val))

  systems <- get_all_systems(doc2)
  if (is.null(systems) || nrow(systems) == 0) {
    message("Skip (no systems available): ", crop_name)
    return(NULL)
  }

  message("Processing ", crop_name, " with ", nrow(systems), " production system(s)...")

  crop_inputs <- map_dfr(seq_len(nrow(systems)), function(j) {
    sys_val   <- systems$system_value[j]
    sys_label <- systems$system_label[j]

    if (j > 1) Sys.sleep(POLITE_DELAY_MS / 1000)

    # state=3 -> select system
    doc3 <- post_state_html(list(state = 3, cropSysId = sys_val))

    # set specifications
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

    if (!page_matches_crop(doc8, crop_name)) {
      selected_val <- html_attr(
        html_node(doc8, xpath = "//select[@name='cropId']/option[@selected]"),
        "value"
      )
      message("Session state mismatch: expected '", crop_name, "' (", sys_label, ")",
              " but page has selected cropId: '", selected_val, "' — returning NA row")
      return(na_input_row(crop_name, sys_label))
    }

    out <- parse_input_items(doc8, crop_name, sys_label)
    if (is.null(out)) {
      message("No input items parsed for: ", crop_name, " (", sys_label, ")")
      return(na_input_row(crop_name, sys_label))
    }
    out
  })

  crop_inputs
})

# 5) Final formatting
inputs_final <- inputs_raw |>
  arrange(crop, production_system, item_label)

write.csv(inputs_final, here("fertilizers_and_others_test.csv"))

# --------------------------
# Check fertilizers
# --------------------------
# Pattern that matches mineral and organic fertilizers (and lime as soil amendment).
# Remove "Kohlensaurer Kalk" from the pattern if you want to exclude lime.
fert_pattern <- "Dünger|Gülle|Gärrest|Kalkammonsalpeter|Diammonphosphat|Kali-Magnesia|Kohlensaurer Kalk"

fertilizers_only <- inputs_final |>
  dplyr::filter(grepl(fert_pattern, item_label))

# Inventory of fertilizers found and the units used for the "amount" column
fertilizer_units <- fertilizers_only |>
  dplyr::count(item_label, amount_unit, sort = TRUE)

# Inventory with both units (amount + price) and count
fertilizer_inventory <- fertilizers_only |>
  dplyr::count(item_label, amount_unit, unit_price_unit, sort = TRUE)

cat("\n=== Fertilizers found and their units ===\n")
print(fertilizer_units, n = Inf)

cat("\n=== Fertilizer inventory (amount unit + price unit + count) ===\n")
print(fertilizer_inventory, n = Inf)

write.csv(fertilizers_only, here("fertilizers_only.csv"), row.names = FALSE)
write.csv(fertilizer_units, here("fertilizer_units.csv"), row.names = FALSE)
write.csv(fertilizer_inventory, here("fertilizer_list.csv"), row.names = FALSE)

# --------------------------
# Economic summary: organic vs mineral fertilizers
# --------------------------
# Classify each fertilizer item into a category
fertilizers_classified <- fertilizers_only |>
  dplyr::mutate(
    fert_category = dplyr::case_when(
      grepl("Gülle|Gärrest|Mist", item_label) ~ "organic",
      grepl("Kohlensaurer Kalk", item_label)  ~ "lime",
      TRUE                                    ~ "mineral"
    )
  )

# Summary per (crop, production_system, fert_category):
#   - number of distinct fertilizer products applied
#   - total physical amount (by unit, since units differ between categories)
#   - total cost in €/ha (always comparable)
fertilizer_summary <- fertilizers_classified |>
  dplyr::group_by(crop, production_system, fert_category, amount_unit) |>
  dplyr::summarise(
    n_products = dplyr::n(),
    total_amount = sum(amount, na.rm = TRUE),
    total_cost_eur_ha = sum(total_cost, na.rm = TRUE),
    .groups = "drop"
  )

# A simpler view: total cost per category per (crop, system), ignoring units
fertilizer_cost_summary <- fertilizers_classified |>
  dplyr::group_by(crop, production_system, fert_category) |>
  dplyr::summarise(
    n_products = dplyr::n(),
    total_cost_eur_ha = sum(total_cost, na.rm = TRUE),
    .groups = "drop"
  ) |>
  tidyr::pivot_wider(
    names_from = fert_category,
    values_from = c(n_products, total_cost_eur_ha),
    values_fill = 0
  )

cat("\n=== Fertilizer summary by category (with units) ===\n")
print(fertilizer_summary, n = 30)

cat("\n=== Fertilizer cost summary (organic vs mineral vs lime, €/ha) ===\n")
print(fertilizer_cost_summary, n = 30)

write.csv(fertilizers_classified,    here("fertilizers_classified.csv"),    row.names = FALSE)
write.csv(fertilizer_summary,        here("fertilizer_summary.csv"),        row.names = FALSE)
write.csv(fertilizer_cost_summary,   here("fertilizer_cost_summary.csv"),   row.names = FALSE)

# Clean up
unlink(cookie_file)
