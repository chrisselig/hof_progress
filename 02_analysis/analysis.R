# 0.0 Libraries
library(dplyr)
library(purrr)
library(janitor)
library(rvest)
library(duckdb)
library(arrow)
library(keyring)
library(AzureStor)
library(DBI)


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

# 1.3 Download Player Ids ----

# Create function that doesn't error out if the file doesn't exist

safety_function <- function(x) {
    tryCatch(
        download.file(x, destfile = file.path("00_data_raw/", basename(x))),
        error = function(e) e
    )
}

numbers <- c('0','1','2','3','4','5','6','7','8','9')

# Download people files that have a number in name
paste0('https://raw.githubusercontent.com/chadwickbureau/register/master/data/people-',numbers,'.csv') |>
    purrr::map(
        \(x)  safety_function(x)
    )


# Download files that have letters in the name
paste0('https://raw.githubusercontent.com/chadwickbureau/register/master/data/people-',letters,'.csv') |>
    purrr::map(
        \(x)  safety_function(x)
    )

# 1.4 Baseball Reference Data ----
# This is daily data so needs a pipeline that runs daily



# 2.0 Read In Data ----

# 2.1 Season Level Stats ----
#player_career_pitching_raw <- dbGetQuery(con, "SELECT * FROM metrics_player_career_pitching")
player_career_offense <- dbGetQuery(
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
player_season_offense <- dbGetQuery(
    con, 
    "SELECT
        player_id,
        season,
        league
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

# 2.4 Fangraph fWAR data ----
fg_war_raw <- readr::read_csv("00_data_raw/fWAR_2023.csv")

# 2.5 Chadwickbureau Player Reference id Data ----

# read all files that have people- in name
chadwickbureau_player_reference_raw <- list.files("00_data_raw/", pattern = "people-") |>
    purrr::map(
        \(x) readr::read_csv(file.path("00_data_raw/", x), col_types = readr::cols(.default = "c"))
        ) |>
    purrr::reduce(
        dplyr::bind_rows
        )

# 3.0 Clean Data & Combine Data ----

# 3.1 Clean Reference Data ----
player_bio <- player_bio_raw |> 
    janitor::clean_names() |> 
    rename(player_id = playerid)

relatives <- relatives_raw |> 
    janitor::clean_names()

teams <- teams_raw |> 
    janitor::clean_names()

# 3.2 Clean Fangraph fWAR Data ----
fg_war <- fg_war_raw |> 
    janitor::clean_names()
    # 
# fg_names <- fg_war |> 
#     select(name)
# 
# player_bio_names <- player_bio |> 
#     select(player_id, first, last, nickname) |> 
#     mutate(name = paste(nickname, last, sep = " ")) #|> 
#     # select(-first, -last)
# 
# matched_names <- fg_names |> 
#     inner_join(player_bio_names, by = "name") |> 
#     select(name, player_id)
# 
# unmatched_names <- fg_names |> 
#     anti_join(player_bio_names, by = "name") |> 
#     select(name) |>
#     distinct(name)
# 
# fuzzy_matched_names <- unmatched_names |> 
#     mutate(
#         player_id = stringdist::amatch(name, player_bio_names$name, maxDist = 3),
#         player_id = as.character(player_id)
#     )
# 
# # Remove suffix to match
# split_suffix_match_test <- fuzzy_matched_names |>
#     filter(is.na(player_id)) |> 
#     # remove rows with (Unknown) in name
#     filter(!grepl("(Unknown)", name)) |> 
#     # Create new column that finds Sr. or Jr. in name column. Remove from name column and add to "suffix" column
#     mutate(
#         suffix = stringr::str_extract(name, "(Sr.|Jr.)"),
#         name = stringr::str_remove(name, "(Sr.|Jr.)")
#     ) |> 
#     mutate(
#         player_id = stringdist::amatch(name, player_bio_names$name, maxDist = 3)    
#     )
#     
# suffix_match <- split_suffix_match_test |> 
#     filter(!is.na(player_id)) |> 
#     select(name, suffix, player_id) |> 
#     mutate(
#         name = paste(name, suffix, sep = " "),
#         player_id = as.character(player_id)
#     )
# 
# unmatched_names <- split_suffix_match_test |> 
#     filter(is.na(player_id))
# 
# 
# 
# # Combine all player_id tables together
# player_id_matched_names <- matched_names |>
#     bind_rows(fuzzy_matched_names) |>
#     bind_rows(suffix_match) |> 
#     select(-suffix) |> 
#     distinct(name,player_id)
    
# Combine updates names with fg_war
fg_war |>
    left_join(player_id_matched_names, by = "name") |> View()
    # inner_join(player_bio_names, by = "player_id") |> 
    # select(name, player_id)

# 9.0 Reference Data to parquet ----
arrow::write_parquet(player_bio, "00_data_clean/player_bio.parquet")
# arrow::write_parquet(coaches_raw, "00_data_clean/coaches.parquet")
arrow::write_parquet(relatives, "00_data_clean/relatives.parquet")
arrow::write_parquet(teams, "00_data_clean/teams.parquet")

# 9.2 Career Level Stats to parquet ----
arrow::write_parquet(player_career_offense, "00_data_clean/player_career_offense.parquet")
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

# 9.4 Load Data to Motherduck ----
# Create database connection
motherduck_con <- duckdb::dbConnect(duckdb(),paste0('md:baseball?motherduck_token=', keyring::key_get("motherduck_token")))


# Write data to table
dbWriteTable(motherduck_con, "relatives", relatives,overwrite = TRUE)
dbWriteTable(motherduck_con, "player_bio", player_bio,overwrite = TRUE)
dbWriteTable(motherduck_con, "teams", teams,overwrite = TRUE)
dbWriteTable(motherduck_con, "player_career_offense", player_career_offense,overwrite = TRUE)
dbWriteTable(motherduck_con, "player_season_offense", player_season_offense,overwrite = TRUE)

duckdb::dbDisconnect(motherduck_con)

# 99.0 ----
# Remove Raw Data Files
# get list of variables in environment that have _raw in the name
#list = ls()

#grep("_raw", ls())
# [1] "coaches_raw"                "hof_list_raw"               "player_bio_raw"             "player_career_fielding_raw"
