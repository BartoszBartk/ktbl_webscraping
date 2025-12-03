# Webscraping data from KTBL database
This repository contains scripts used to get data on crop yields, prices and site- and management-specific costs from the Leistungs-Kosten-Rechner (LKR) Pflanzenbau of the KTBL (https://daten.ktbl.de/dslkrpflanze).

There are two scripts in this database:

* ktbl_multisystem_loop.R defines a loop (which invokes the other script) that allows one to extract data for multiple combinations of site/farm parameters.
* ktbl_multisystem_scraper.R defines a procedure to extract a set of relevant data via the LKR web interface. This script is heavily based on Christoph Pahmeyer's approach (https://github.com/fruchtfolge/KTBL-APIs), which was translated to R by Malin Gütschow and Bartosz Bartkowski, relying to quite some extent on ChatGPT.

Note: There are still some errors or at least challenges for the scraper in its current version. For example, for some crops (e.g. soy, grass), the LKR provides two yield components – only one of those is currently scraped. Also, the extraction of organic fertilizer only works for Gülle (manure), whereas for some crops, especially under organic production and bioenergy production, the LKR assumes other sources of organic fertilizer (e.g. compost or biogas residuals).

Authors: Bartosz Bartkowski (bartosz.bartkowski@ufz.de) and Malin Gütschow (malin-sophie.guetschow@ufz.de).
