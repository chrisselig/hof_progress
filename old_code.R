# # List of Data Sources
# urls <- list("https://www.retrosheet.org/gamelogs/gl1871_99.zip",
#           "https://www.retrosheet.org/gamelogs/gl1900_19.zip",
#           "https://www.retrosheet.org/gamelogs/gl1920_39.zip",
#           "https://www.retrosheet.org/gamelogs/gl1940_59.zip",
#           "https://www.retrosheet.org/gamelogs/gl1960_69.zip",
#           "https://www.retrosheet.org/gamelogs/gl1970_79.zip",
#           "https://www.retrosheet.org/gamelogs/gl1980_89.zip",
#           "https://www.retrosheet.org/gamelogs/gl1990_99.zip",
#           "https://www.retrosheet.org/gamelogs/gl2000_09.zip",
#           "https://www.retrosheet.org/gamelogs/gl2010_19.zip",
#           "https://www.retrosheet.org/gamelogs/gl2020_23.zip"
#           )


# urls |> 
#     purrr::map(
#         \(x)  download.file(x, destfile = file.path("00_data/", basename(x)))
#         )
# 
# # Unzip data
# zipped_files <- list.files("00_data/", full.names = TRUE, pattern = "*.zip") 
# 
# zipped_files |> 
#     purrr::map(
#         \(x) unzip(x, exdir = "00_data/")
#     )

# Remove zipped files from 00_data directory
# file.remove(zipped_files)

# Read in data

# txt_files <- list.files("00_data/", full.names = TRUE, pattern = "*.txt")
# 
# raw_data <- txt_files |>
#     map_dfr(
#         \(x) read.table(x, header = FALSE, sep = ",", skipNul = TRUE)
#     )
