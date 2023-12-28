# libraries

library(dplyr)
# library(baseballr)
library(httr2)
library(purrr)
library(janitor)
library(rvest)
library(duckdb)
# library(odbc)


# Download data

hof_list_raw <- rvest::read_html('https://en.wikipedia.org/wiki/List_of_members_of_the_Baseball_Hall_of_Fame') |> 
    rvest::html_nodes('table') |> 
    rvest::html_table() |>
    (\(.) .[[3]])() |> 
    janitor::clean_names()


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


# Let's find season-level statistics for all pitchers and put it in a DataFrame
player_career_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_pitching")
player_career_offense_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_offense")
player_career_fielding_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_fielding")

# Get list of players from duckdb database

# Need to combine HOF data with player data to identify to get player ids