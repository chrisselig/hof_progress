# libraries
library(dplyr)
# library(baseballr)
# library(httr2)
library(purrr)
library(janitor)
library(rvest)
library(duckdb)
# library(odbc)


# 1.0 Download data ----

# 1.1 Baseball Computer Data ----
# This duckdb database comes from: https://baseball.computer/
# Create a database file on disk
con <- dbConnect(duckdb(), dbdir = "retro_baseball_stats.db")

# Enable remote access
dbExecute(con, "INSTALL httpfs")
dbExecute(con, "LOAD httpfs")
# This ATTACH command only needs to be run once on an existing database and will fail
# if run twice, but you can safely ignore the error in that case
dbExecute(con, "ATTACH 'https://data.baseball.computer/dbt/bc_remote.db' (READ_ONLY)")
dbExecute(con, "USE bc_remote")
dbExecute(con, "USE main_models")

# Tables of interest
# metrics_player_career_offense
# metrics_player_career_pitching
# metrics_player_career_fielding

# metrics_player_season_league_offense
# metrics_player_season_league_pitching
# metrics_player_season_league_fielding

# Park factors contains park factors calculated using a batter-pitcher-matched-pair methodology (and a more standard aggregate methodology as a fallback for years with insufficient data).
# park_factors 

# 1.2 Player Info and other Reference Data ----
# https://github.com/chadwickbureau/retrosheet/tree/official
urls <- list(
    'https://raw.githubusercontent.com/chadwickbureau/retrosheet/official/reference/biofile.csv', # player bio
    'https://raw.githubusercontent.com/chadwickbureau/retrosheet/official/reference/coaches.csv', # coaches
    'https://raw.githubusercontent.com/chadwickbureau/retrosheet/official/reference/relatives.csv', # relatives
    'https://raw.githubusercontent.com/chadwickbureau/retrosheet/official/reference/teams.csv' # teams names
    )


urls |>
    purrr::map(
        \(x)  download.file(x, destfile = file.path("00_data/", basename(x)))
        )

# 2.0 Read In Data ----
# 2.1 Season Level Stats ----
player_career_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_pitching")
player_career_offense_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_offense")
player_career_fielding_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_fielding")

# 2.2 Season Level Stats ----
player_season_league_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_season_league_pitching")
player_season_league_offense_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_season_league_offense")
player_season_league_fielding_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_season_league_fielding")

# 2.3 Retrosheet Reference Data ----

reference_data_list <- urls |>
    purrr::map(
        \(x)  readr::read_csv(file.path("00_data/", basename(x)))
        )
player_bio_raw <- reference_data_list[[1]]
coaches_raw <- reference_data_list[[2]]
relatives_raw <- reference_data_list[[3]]
teams_raw <- reference_data_list[[4]]

# 3.0 Clean Data & Combine Data ----
# 3.1 Clean Reference Data ----
player_bio_tidy <- player_bio_raw |> 
    janitor::clean_names()

relatives_tidy <- relatives_raw |> 
    janitor::clean_names()

teams_tidy <- teams_raw |> 
    janitor::clean_names()

# Need to combine HOF data with player data to identify to get player ids

# Remove Raw Data Files
# get list of variables in environment that have _raw in the name
#list = ls()

#grep("_raw", ls())
# [1] "coaches_raw"                "hof_list_raw"               "player_bio_raw"             "player_career_fielding_raw"
