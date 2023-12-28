# libraries

library(dplyr)
# library(baseballr)
library(httr2)
library(purrr)
library(janitor)
library(rvest)
library(duckdb)
# library(odbc)


# Download data ----

#0.1 List of Hall of Famer Data ----
raw <- rvest::read_html('https://en.wikipedia.org/wiki/List_of_members_of_the_Baseball_Hall_of_Fame') |> 
    rvest::html_nodes('table') |> 
    rvest::html_table() |>
    (\(.) .[[3]])() |> 
    janitor::clean_names()

# 0.2 Baseball Computer Data ----
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

# Let's find season-level statistics for all pitchers and put it in a DataFrame
player_career_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_pitching")
player_career_offense_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_offense")
player_career_fielding_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_fielding")

# 0.3 Player Info and other Reference Data ----
# https://github.com/chadwickbureau/retrosheet/tree/official
urls <- list(
    'https://github.com/chadwickbureau/retrosheet/blob/40d79e757aa348f6e11303e8bd1d2a966bfe877f/reference/biofile.csv', # player bio
    'https://github.com/chadwickbureau/retrosheet/blob/40d79e757aa348f6e11303e8bd1d2a966bfe877f/reference/coaches.csv', # coaches
    'https://github.com/chadwickbureau/retrosheet/blob/40d79e757aa348f6e11303e8bd1d2a966bfe877f/reference/relatives.csv', # relatives
    'https://github.com/chadwickbureau/retrosheet/blob/40d79e757aa348f6e11303e8bd1d2a966bfe877f/reference/teams.csv' # teams names
    )

urls |>
    purrr::map(
        \(x)  download.file(x, destfile = file.path("00_data/", basename(x)))
        )

    

# Get list of players from duckdb database

# Need to combine HOF data with player data to identify to get player ids