#!/usr/bin/Rscript


#libs, yo!
# library(DBI)
# library(RPostgres)
# library(janitor)
library(tidyverse)


#Reading and cleaning the data! BIG DATA!
intel <- read.csv("clean.csv")		#Reading
intel <- janitor::clean_names(intel)	#Cleaning the column names

# unavoidable manual corrections
intel$lithographie[intel$lithographie == "Intel 7"] <- "10 nm"
intel$x4k_unterstutzung <- gsub("Yes \\|  at ", "", intel$x4k_unterstutzung)
intel$x4k_unterstutzung <- gsub("Hz", " Hz", intel$x4k_unterstutzung)
intel$cache <- gsub(" Intel® Smart Cache", "", intel$cache)
intel$e_core_base_frequency[intel$e_core_base_frequency == "900 MHz"] <- "0.9 GHz"
intel$bus_taktfrequenz <- gsub("\\/", "_per_", intel$bus_taktfrequenz)
intel$intel_turbo_boost_max_technology_3_0_frequency <- gsub(" \\| ", ".", intel$intel_turbo_boost_max_technology_3_0_frequency)
intel$grundtaktfrequenz_des_prozessors <- gsub(" \\| ", ".", intel$grundtaktfrequenz_des_prozessors)

cols_of_interest <- 
  c(
    "max_turbo_taktfrequenz", "lithographie", "intel_turbo_boost_technik_2_0_taktfrequenz", 
    "grundtaktfrequenz_des_prozessors", "cache", "bus_taktfrequenz", "verlustleistung_tdp", 
    "intel_turbo_boost_max_technology_3_0_frequency", "single_p_core_turbo_frequency", 
    "single_e_core_turbo_frequency", "e_core_base_frequency", "total_l2_cache", 
    "processor_base_power", "maximum_turbo_power", "grundtaktfrequenz_der_grafik", 
    "max_dynamische_grafikfrequenz", "max_videospeicher_der_grafik", "x4k_unterstutzung"
  )

# Extract units and append to column name
# ('cause units will be deleted later so only numeric values remain)
# unit can always be found as the last letters after the last whitespace
column_units <- 
  apply(intel[cols_of_interest], 2, function(x) {
    unit <- unique(gsub(".*\\s([a-zA-Z_]+)$", "\\1", x))
    unit[unit != ""] # ignore empty cells
  })

# remove units, and make columns numeric
intel <- 
  intel %>% 
  mutate(
    across(
      cols_of_interest, 
      ~as.numeric(
        ifelse(
          gsub("\\s[a-zA-Z_]+$", "", .) == "", 
          NA, 
          gsub("\\s[a-zA-Z_]+$", "", .)
        )
      )
    )
  )

# append units to column names
intel <- 
  intel %>% 
  rename_at(
    vars(names(column_units)), ~ 
      paste0(names(column_units), "_", column_units)
  )

#database shizz (Raw SQL is scary)
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "bda",
  host = "localhost" ,
  port = 5432,
  user = "bda",
  password = "bda",
)

dbWriteTable(con, "intel", intel, row.names = FALSE, overwrite = TRUE)
dbDisconnect(con)
