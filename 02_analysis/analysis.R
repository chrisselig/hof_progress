# 0.0 Libraries
library(dplyr)
library(purrr)
library(janitor)
library(rvest)
library(duckdb)
library(arrow)
library(keyring)
library(AzureStor)


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
        \(x)  download.file(x, destfile = file.path("00_data_raw/", basename(x)))
        )

# 1.3 Baseball Reference Data ----
# This is daily data so needs a pipeline that runs daily

# 2.0 Read In Data ----

# 2.1 Season Level Stats ----
#player_career_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_pitching")
player_career_offense_raw <- dbGetQuery(
    con,
    "SELECT 
        player_id,
        plate_appearances,
        at_bats,
        hits,
        singles,
        doubles,
        triples,
        home_runs,
        total_bases,
        strikeouts,
        walks,
        intentional_walks,
        hit_by_pitches,
        sacrifice_hits,
        sacrifice_flies,
        reached_on_errors,
        reached_on_interferences,
        inside_the_park_home_runs,
        infield_hits,
        on_base_opportunities,
        on_base_successes,
        runs_batted_in,
        grounded_into_double_plays,
        batting_outs,
        balls_in_play,
        balls_batted,
        bunts,
        runs,
        times_reached_base,
        stolen_bases,
        caught_stealing,
        picked_off,
        picked_off_caught_stealing,
        outs_on_basepaths,
        pitches,
        swings,
        swings_with_contact,
        left_on_base,
        left_on_base_with_two_outs,
        batting_average,
        on_base_percentage,
        slugging_percentage,
        on_base_plus_slugging,
        isolated_power,
        home_run_rate,
        walk_rate,
        strikeout_rate,
        stolen_base_percentage,
        fly_ball_rate,
        line_drive_rate,
        pop_up_rate,
        ground_ball_rate,
        coverage_weighted_air_ball_batting_average,
        coverage_weighted_ground_ball_batting_average,
        coverage_weighted_fly_ball_batting_average,
        coverage_weighted_line_drive_batting_average,
        coverage_weighted_pop_up_batting_average,
        pulled_rate_outs,
        pulled_rate_hits,
        pulled_rate,
        opposite_field_rate_outs,
        opposite_field_rate_hits,
        opposite_field_rate
    FROM metrics_player_career_offense"
    )

# player_career_fielding_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_fielding")

# 2.2 Season Level Stats ----
#player_season_league_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_season_league_pitching")
player_season_league_offense_raw <- dbGetQuery(
    con, 
    "SELECT
        player_id,
        plate_appearances,
        at_bats,
        hits,
        singles,
        doubles,
        triples,
        home_runs,
        total_bases,
        strikeouts,
        walks,
        intentional_walks,
        hit_by_pitches,
        sacrifice_hits,
        sacrifice_flies,
        reached_on_errors,
        reached_on_interferences,
        inside_the_park_home_runs,
        infield_hits,
        on_base_opportunities,
        on_base_successes,
        runs_batted_in,
        grounded_into_double_plays,
        batting_outs,
        balls_in_play,
        balls_batted,
        bunts,
        runs,
        times_reached_base,
        stolen_bases,
        caught_stealing,
        picked_off,
        picked_off_caught_stealing,
        outs_on_basepaths,
        pitches,
        swings,
        swings_with_contact,
        left_on_base,
        left_on_base_with_two_outs,
        batting_average,
        on_base_percentage,
        slugging_percentage,
        on_base_plus_slugging,
        isolated_power,
        home_run_rate,
        walk_rate,
        strikeout_rate,
        stolen_base_percentage,
        fly_ball_rate,
        line_drive_rate,
        pop_up_rate,
        ground_ball_rate,
        coverage_weighted_air_ball_batting_average,
        coverage_weighted_ground_ball_batting_average,
        coverage_weighted_fly_ball_batting_average,
        coverage_weighted_line_drive_batting_average,
        coverage_weighted_pop_up_batting_average,
        pulled_rate_outs,
        pulled_rate_hits,
        pulled_rate,
        opposite_field_rate_outs,
        opposite_field_rate_hits,
        opposite_field_rate
    FROM metrics_player_season_league_offense"
    )

# player_season_league_fielding_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_season_league_fielding")

# 2.3 Retrosheet Reference Data ----

reference_data_list <- urls |>
    purrr::map(
        \(x)  readr::read_csv(file.path("00_data_raw/", basename(x)))
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


# 9.0 Reference Data to parquet ----
arrow::write_parquet(player_bio_tidy, "00_data_clean/player_bio.parquet")
# arrow::write_parquet(coaches_raw, "00_data_clean/coaches.parquet")
arrow::write_parquet(relatives_tidy, "00_data_clean/relatives.parquet")
arrow::write_parquet(teams_tidy, "00_data_clean/teams.parquet")

# 9.2 Career Level Stats to parquet ----
arrow::write_parquet(player_career_offense_raw, "00_data_clean/player_career_offense.parquet")
# arrow::write_parquet(player_career_fielding_raw, "00_data_clean/player_career_fielding.parquet")
# arrow::write_parquet(player_career_pitching_raw, "00_data_clean/player_career_pitching.parquet")

# 9.3 Write Data for Shiny App to Azure ----

# Single file upload
upload_to_url("00_data_clean/player_bio.parquet",
              "https://baseballdata.blob.core.windows.net/bronzebaseball/player_bio.parquet",
              key=keyring::key_get("azure_storage_account_key")
)

# Multiple file upload
list.files("00_data_clean") |>
    purrr::map(
        \(x) upload_to_url(
            file.path("00_data_clean/", x),
            paste0("https://baseballdata.blob.core.windows.net/bronzebaseball/", x),
            key=keyring::key_get("azure_storage_account_key")
        )
    )

# 99.0 ----
# Remove Raw Data Files
# get list of variables in environment that have _raw in the name
#list = ls()

#grep("_raw", ls())
# [1] "coaches_raw"                "hof_list_raw"               "player_bio_raw"             "player_career_fielding_raw"
