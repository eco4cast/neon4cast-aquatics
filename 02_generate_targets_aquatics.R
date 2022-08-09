message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

avro_file_directory <- "/home/rstudio/data/aquatic_avro"
EDI_file_directory <- "/home/rstudio/data/aquatic_EDI"

readRenviron("~/.Renviron") # compatible with littler
Sys.setenv("NEONSTORE_HOME" = "/home/rstudio/data/neonstore")




# Sys.setenv("NEONSTORE_HOME" = "/home/rstudio/data/neonstore") #Sys.setenv("NEONSTORE_HOME" = "/efi_neon_challenge/neonstore")
#Sys.setenv("NEONSTORE_DB" = "home/rstudio/data/neonstore")    #Sys.setenv("NEONSTORE_DB" = "/efi_neon_challenge/neonstore")

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")

Sys.setenv(TZ = 'UTC')
## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)
library(contentid)
library(sparklyr)
library(sparkavro)
source('R/avro_functions.R')
source('R/data_processing.R')
# spark_install(version = '3.0')

`%!in%` <- Negate(`%in%`) # not in function

message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-aquatics/master/Aquatic_NEON_Field_Site_Metadata_20220727.csv")

nonwadable_rivers <- sites$field_site_id[(which(sites$field_site_subtype == "Non-wadeable River"))]
lake_sites <- sites$field_site_id[(which(sites$field_site_subtype == "Lake"))]
stream_sites <- sites$field_site_id[(which(sites$field_site_subtype == "Wadeable Stream"))]

#======================================================#

#message("Downloading: DP1.20288.001")

#neonstore::neon_download("DP1.20288.001",site = sites$field_site_id, type = "basic")
#neonstore::neon_store(table = "waq_instantaneous", n = 50)
#message("Downloading: DP1.20264.001")
#neonstore::neon_download("DP1.20264.001", site =  sites$field_site_id, type = "basic")
#neonstore::neon_store(table = "TSD_30_min")
#message("Downloading: DP1.20053.001")
#neonstore::neon_download("DP1.20053.001", site =  sites$field_site_id, type = "basic")
#neonstore::neon_store(table = "TSW_30min")

## Load data from raw files
# message("neon_table(table = 'waq_instantaneous')")
# wq_raw <- neonstore::neon_table(table = "waq_instantaneous", site = stream_sites[1:6]) 
# message("neon_table(table = 'TSD_30_min')")
# temp_bouy <- neonstore::neon_table("TSD_30_min", site = nonwadable_rivers)
# message("neon_table(table = 'TSW_30min')")
# temp_prt <- neonstore::neon_table("TSW_30min", site = stream_sites) 


message("#### Generate WQ table #############")
neon <- arrow::s3_bucket("neon4cast-targets/neon",
                         endpoint_override = "data.ecoforecast.org",
                         anonymous = TRUE)
# list tables with `neon$ls()`
wq_portal <- arrow::open_dataset(neon$path("waq_instantaneous-basic-DP1.20288.001")) %>%   # waq_instantaneous
  dplyr::filter(siteID %in% sites$field_site_id) %>%
  dplyr::select(siteID, startDateTime, sensorDepth,
                dissolvedOxygen,dissolvedOxygenExpUncert,dissolvedOxygenFinalQF, 
                chlorophyll, chlorophyllExpUncert,chlorophyllFinalQF) %>%
  dplyr::mutate(sensorDepth = as.numeric(sensorDepth),
                dissolvedOxygen = as.numeric(dissolvedOxygen),
                dissolvedOxygenExpUncert = as.numeric(dissolvedOxygenExpUncert),
                chla = as.numeric(chlorophyll),
                chlorophyllExpUncert = as.numeric(chlorophyllExpUncert)) %>%
  rename(site_id = siteID) %>% 
  dplyr::filter(((sensorDepth > 0 & sensorDepth < 1)| is.na(sensorDepth))) %>%
  dplyr::collect()

print("here")

wq_portal <- wq_portal %>% # sensor depth of NA == surface?
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(time = as_date(startDateTime)) %>%
  # QF (quality flag) == 0, is a pass (1 == fail), 
  # make NA so these values are not used in the mean summary
  dplyr::mutate(dissolvedOxygen = ifelse(dissolvedOxygenFinalQF == 0, dissolvedOxygen, NA),
                chla = ifelse(chlorophyllFinalQF == 0, chla, NA)) %>% 
  dplyr::group_by(site_id, time) %>%
  dplyr::summarize(oxygen__observed = mean(dissolvedOxygen, na.rm = TRUE),
                   chla__observed = mean(chla, na.rm = TRUE),
                   oxygen__sample_error = se(dissolvedOxygen),
                   chla__sample_error = se(chla),
                   count = sum(!is.na(dissolvedOxygen)),
                   chla__measure_error = mean(chlorophyllExpUncert, na.rm = TRUE)/sqrt(count),
                   oxygen__measure_error = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
  dplyr::select(time, site_id, 
                oxygen__observed, chla__observed, 
                oxygen__sample_error, chla__sample_error, 
                oxygen__measure_error, chla__measure_error) %>% 
  pivot_longer(cols = !c(time, site_id), names_to = c("variable", "stat"), names_sep = "__") %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  filter(!(variable == "chla" & site_id %in% stream_sites))
  
#====================================================#
##### low latency WQ data =======
message("# download the 24/48hr provisional data from the Google Cloud")

# where should these files be saved?

fs::dir_create(avro_file_directory) # ignores existing directories unlike dir.create()

# need to figure out which month's data are required
# what is in the NEON store db?
cur_wq_month <- format(as.Date(max(wq_portal$time)), "%Y-%m")
# what is the next month from this plus the current month? These might be the same
new_month_wq <- unique(format(c((as.Date(max(wq_portal$time)) %m+% months(1)), (Sys.Date() - days(2))), "%Y-%m"))


# Start by deleting superseded files
# Files that have been supersed by the NEON store files can be deleted from the relevent repository
# Look in each repository to see if there are files that match the current maximum month of the NEON
      # store data

delete.neon.avro(months = cur_wq_month,
                 sites = unique(sites$field_site_id), 
                 path = avro_file_directory)


# Download any new files from the Google Cloud
download.neon.avro(months = new_month_wq, 
                   sites = unique(sites$field_site_id), 
                   data_product = '20288',  # WQ data product
                   path = avro_file_directory)

# Read in the new files to append to the NEONstore data
# connect to spark locally 
sc <- sparklyr::spark_connect(master = "local", version = '3.0')

# The variables (term names that should be kept)
wq_vars <- c('siteName',
             'startDate',
             'dissolvedOxygen',
             'dissolvedOxygenExpUncert',
             'dissolvedOxygenFinalQF',
             'chlorophyll',
             'chlorophyllExpUncert',
             'chlorophyllFinalQF')
columns_keep <- c('siteName', 'termName', 'startDate', 'Value', 'verticalIndex')

# Generate a list of files to be read
wq_avro_files <- paste0(avro_file_directory, '/',
                        list.files(path = paste0(avro_file_directory), 
                                   pattern = '*20288', 
                                   recursive = T))

# Read in each of the files and then bind by rows
wq_avro_df <- purrr::map_dfr(.x = wq_avro_files, ~ read.avro.wq(sc= sc, path = .x))


# Combine the avro files with the portal data
wq_full <- dplyr::bind_rows(wq_portal, wq_avro_df) %>%
  dplyr::arrange(site_id, time)


#==============================#

message("##### WQ QC protocol =======")
# additional QC steps implemented (FO, 2022-07-13)
##### check 1 Gross range tests on DO and chlorophyll
# DO ranges for each sensor and each season
DO_max <- 15 # gross max
DO_min <- 2 # gross min

# chlorophyll ranges
chla_max <- 200
chla_min <- 0

# GR flag will be true if either the DO concentration or the chlorophyll are 
# outside the ranges specified about

wq_cleaned <- wq_full %>%
  dplyr::mutate(observed = ifelse(is.na(observed),
                                     observed, ifelse(observed >= DO_min & observed <= DO_max & variable == 'oxygen', 
                                                         observed, ifelse(observed >= chla_min & observed <= chla_max & variable == 'chla', observed, NA)))) %>%
  # manual cleaning based on visual inspection
  dplyr::mutate(observed = ifelse(site_id == "MAYF" & 
                                       between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                                       variable == "oxygen", NA, observed),
                observed = ifelse(site_id == "WLOU" &
                                       !between(observed, 7.5, 11) & 
                                       variable == "oxygen", NA, observed),
                observed = ifelse(site_id == "BARC" & 
                                       observed < 4 &
                                       variable == "oxygen", NA, observed),
                
                measure_error = ifelse(site_id == "MAYF" &
                                 between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                                 variable == "oxygen", NA, measure_error),
                measure_error = ifelse(site_id == "WLOU" &
                                 !between(observed, 7.5, 11) & 
                                 variable == "oxygen", NA, measure_error),
                measure_error = ifelse(site_id == "BARC" & 
                                 observed < 4 &
                                 variable == "oxygen", NA, measure_error),
                
                sample_error = ifelse(site_id == "MAYF" & 
                              between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                              variable == "oxygen", NA, sample_error),
                sample_error = ifelse(site_id == "WLOU" &
                              !between(observed, 7.5, 11) & 
                              variable == "oxygen", NA, sample_error),
                sample_error = ifelse(site_id == "BARC" & 
                              observed < 4 &
                              variable == "oxygen", NA, sample_error))

#===============================================#
message("#### Generate hourly temperature profiles for lake #############")
message("##### NEON portal data #####")
hourly_temp_profile_portal <- arrow::open_dataset(neon$path("TSD_30_min-basic-DP1.20264.001")) %>%
  # neonstore::neon_table("TSD_30_min", site = sites$field_site_id) %>%
  rename(site_id = siteID,
         depth = thermistorDepth) %>%
  dplyr::filter(site_id %in% lake_sites) %>%
  dplyr::select(startDateTime, site_id, tsdWaterTempMean, depth, tsdWaterTempExpUncert, tsdWaterTempFinalQF, verticalPosition) %>%
  dplyr::collect() %>%
  # errors in the sensor depths reported - see "https://www.neonscience.org/impact/observatory-blog/incorrect-depths-associated-lake-and-river-temperature-profiles"
  # sensor depths are manually assigned based on "vertical position" variable as per table on webpage
  dplyr::mutate(depth = ifelse(site_id == "CRAM" & 
                                           lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                           verticalPosition == 502, 1.75, depth),
                depth = ifelse(site_id == "CRAM" & 
                                           lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                           verticalPosition == 503, 3.45, depth),
                depth = ifelse(site_id == "CRAM" & 
                                           lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                           verticalPosition == 504, 5.15, depth),
                depth = ifelse(site_id == "CRAM" & 
                                           lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                           verticalPosition == 505, 6.85, depth),
                depth = ifelse(site_id == "CRAM" & 
                                           lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                           verticalPosition == 506, 8.55, depth),
                depth = ifelse(site_id == "CRAM" & 
                                           lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                           verticalPosition == 505, 10.25, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 502, 0.55, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 503, 1.05, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 504, 1.55, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 505, 2.05, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 506, 2.55, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 507, 3.05, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 502, 0.3, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 503, 0.55, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 504, 0.8, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 505, 1.05, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 506, 1.3, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 507, 1.55, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 508, 2.05, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 509, 2.55, depth),
                depth = ifelse(site_id == "BARC" & 
                                           (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") | 
                                              lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                           verticalPosition == 510, 3.05, depth)) %>%
  dplyr::filter((tsdWaterTempFinalQF == 0 | (tsdWaterTempFinalQF == 1 & as_date(startDateTime) > as_date("2020-07-01")))) %>% 
  dplyr::mutate(time = ymd_h(format(startDateTime, "%y-%m-%d %H")),
                depth = round(depth, 1)) %>% # round to the nearest 0.1 m
  group_by(site_id, depth, time) %>%
  dplyr::summarize(temperature__observed = mean(tsdWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(tsdWaterTempMean)),
                   temperature__sample_error = se(tsdWaterTempMean),
                   temperature__measure_error = mean(tsdWaterTempExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
  dplyr::select(time, site_id, temperature__observed, depth,
                temperature__sample_error, temperature__measure_error) %>%
  pivot_longer(names_to = c('variable','stat'),
               names_sep = '__',
               cols = c(temperature__observed,temperature__sample_error, temperature__measure_error)) %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  # include first QC of data
  QC.temp(df = ., range = c(-5, 40), spike = 5, by.depth = T) %>%
  mutate(measure_error = ifelse(is.na(observed), NA, measure_error),
         data_source = 'NEON_portal')

message("##### Sonde EDI data #####")
  # Only 6 lake sites available on EDI
edi_url_lake <- c("https://pasta.lternet.edu/package/data/eml/edi/1071/1/7f8aef451231d5388c98eef889332a4b",
                  "https://pasta.lternet.edu/package/data/eml/edi/1071/1/2c8893684d94b9a52394060a76cab798", 
                  "https://pasta.lternet.edu/package/data/eml/edi/1071/1/770e2ab9d957991a787a2f990d5a2fad",
                  "https://pasta.lternet.edu/package/data/eml/edi/1071/1/2e52d63ba4dc2040d1e5e2d11114aa93",
                  "https://pasta.lternet.edu/package/data/eml/edi/1071/1/60df35a34bb948c0ca5e5556d129aa98", 
                  "https://pasta.lternet.edu/package/data/eml/edi/1071/1/004857d60d6fe7587b112d714e0380d0")
lake_edi_profile <- c("NEON.D03.BARC.DP0.20005.001.01378.csv",
                      "NEON.D05.CRAM.DP0.20005.001.01378.csv",
                      "NEON.D05.LIRO.DP0.20005.001.01378.csv",
                      "NEON.D09.PRLA.DP0.20005.001.01378.csv",
                      "NEON.D09.PRPO.DP0.20005.001.01378.csv",
                      "NEON.D03.SUGG.DP0.20005.001.01378.csv")

fs::dir_create(EDI_file_directory) # ignores existing directories unlike dir.create()
# Download the data

for(i in 1:length(edi_url_lake)){
  if (!file.exists(file.path(EDI_file_directory,  lake_edi_profile[i]))) {
    if (!dir.exists(dirname(file.path(EDI_file_directory, 
                                      lake_edi_profile[i])))) {
      dir.create(dirname(file.path(EDI_file_directory, 
                                   lake_edi_profile[i])))
    }
    download.file(edi_url_lake[i], destfile = file.path(EDI_file_directory, lake_edi_profile[i]))
  }
}


# List all the files in the EDI directory 
edi_data <- list.files(file.path(EDI_file_directory), full.names = T)
# Get the lake sites subset
edi_lake_files <- c(edi_data[grepl(x = edi_data, pattern= lake_sites[1])],
                     edi_data[grepl(x = edi_data, pattern= lake_sites[2])],
                     edi_data[grepl(x = edi_data, pattern= lake_sites[3])],
                     edi_data[grepl(x = edi_data, pattern= lake_sites[4])],
                     edi_data[grepl(x = edi_data, pattern= lake_sites[5])],
                     edi_data[grepl(x = edi_data, pattern= lake_sites[6])])

# Calculate the hourly average profile 
hourly_temp_profile_EDI <- purrr::map_dfr(.x = edi_lake_files, ~ read.csv(file = .x)) %>%
  rename('site_id' = siteID,
         'depth' = sensorDepth,
         'observed' = waterTemp) %>%
  mutate(startDate  = lubridate::ymd_hm(startDate),
         time = lubridate::ymd_h(format(startDate, '%Y-%m-%d %H')),
         depth = round(depth, digits = 1)) %>%
  group_by(site_id, time, depth) %>%
  summarise(temperature__observed = mean(observed),
            temperature__sample_error = se(observed)) %>%
  pivot_longer(names_to = c('variable','stat'),
               names_sep = '__',
               cols = temperature__observed:temperature__sample_error) %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  mutate(measure_error = NA) %>%
  # include first QC of data
  QC.temp(df = ., range = c(-5, 40), spike = 5, by.depth = T) %>%
  mutate(data_source = 'MS_raw')

message("##### avros data #####")
message("# Download any new files from the Google Cloud")
download.neon.avro(months = new_month_wq, 
                   sites = unique(sites$field_site_id), 
                   data_product = '20264',  # TSD data product
                   path = avro_file_directory)



# The variables (term names that should be kept)
tsd_vars <- c('siteName',
              'startDate',
              'tsdWaterTempMean', 
              'thermistorDepth', 
              'tsdWaterTempExpUncert',
              'tsdWaterTempFinalQF')

columns_keep <- c('siteName', 'termName', 'startDate', 'Value', 'verticalIndex')
thermistor_depths <- readr::read_csv('https://raw.githubusercontent.com/OlssonF/neon4cast-aquatics/master/thermistorDepths.csv', col_types = 'ccd')


# Generate a list of files to be read
tsd_avro_files <- paste0(avro_file_directory, '/',
                         list.files(path = avro_file_directory,
                                    pattern = '*20264', 
                                    recursive = T))
lake_avro_files <- c(tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[1])],
                     tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[2])],
                     tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[3])],
                     tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[4])],
                     tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[5])],
                     tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[6])],
                     tsd_avro_files[grepl(x = tsd_avro_files, pattern= lake_sites[7])])

#sc <- sparklyr::spark_connect(master = "local")
message("# Read in each of the files and then bind by rows")
hourly_temp_profile_avro <- purrr::map_dfr(.x = lake_avro_files,
                                           ~ read.avro.tsd.profile(sc= sc,
                                                                   path = .x,
                                                                   thermistor_depths = thermistor_depths)) %>% 
  # include first QC of data
  QC.temp(df = ., range = c(-5, 40), spike = 5, by.depth = T)  %>%
  mutate(measure_error = ifelse(is.na(observed), NA, measure_error), 
         data_source = 'NEON_pre-portal')






# Combine the three data sources
hourly_temp_profile_lakes <- rbind(hourly_temp_profile_portal, hourly_temp_profile_EDI, hourly_temp_profile_avro) %>%
  arrange(time, site_id, depth) %>%
  group_by(time, site_id, depth) %>%
  summarise(observed = mean(observed, na.rm = T),
            sample_error = mean(sample_error, na.rm = T),
            measure_error = mean(measure_error, na.rm = T))

#======================================================#

message("#### Generate surface (< 1 m) temperature #############")
message("###### Lake temperatures #####")
# Daily surface lake temperatures generated from the hourly profiles created above
daily_temp_surface_lakes <- hourly_temp_profile_lakes %>%
  filter(depth <= 1) %>%
  mutate(time = lubridate::as_date(time)) %>%
  group_by(site_id, time) %>%
  summarise(observed = mean(observed, na.rm = T),
            sample_error = mean(sample_error, na.rm = T),
            measure_error = mean(measure_error, na.rm = T)) %>%
  mutate(variable = 'temperature')     
 
message("##### Stream temperatures #####")
temp_streams_portal <- arrow::open_dataset(neon$path("TSW_30min-basic-DP1.20053.001")) %>% 
  # neonstore::neon_table("TSW_30min", site = sites$field_site_id) %>%
  dplyr::filter(horizontalPosition == "101", # take upstream to match WQ data
                finalQF == 0) %>%  
  dplyr::select(startDateTime, siteID, surfWaterTempMean, surfWaterTempExpUncert, finalQF) %>%
  # horizontal position is upstream or downstream is 101 or 102 horizontal position
  dplyr::collect() %>%
  dplyr::rename(site_id = siteID) 

message("##### Stream temperatures2 #####")
temp_streams_portal <- temp_streams_portal %>%
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(time, site_id) %>%
  dplyr::summarize(temperature__observed = mean(surfWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(surfWaterTempMean)),
                   temperature__sample_error = se(surfWaterTempMean),
                   temperature__measure_error = mean(surfWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::select(time, site_id, temperature__observed, 
                temperature__sample_error, temperature__measure_error) %>%
  pivot_longer(cols = !c(time, site_id), 
               names_to = c("variable", "stat"),
               names_sep = '__') %>%
  pivot_wider(names_from = stat, values_from = value) 

message("##### Stream temperatures3 #####")
temp_streams_portal_QC <- temp_streams_portal %>%
  QC.temp(df = ., range = c(-5, 40), spike = 7, by.depth = F)  %>%
  mutate(measure_error = ifelse(is.na(observed), NA, measure_error))
#===========================================#
  
    #### avros
message("#download the 24/48hr provisional data from the Google Cloud")

# Start by deleting superseded files
# Files that have been superseded by the NEON store files can be deleted from the relevant repository
# Look in each repository to see if there are files that match the current maximum month of the NEON
# store data

delete.neon.avro(months = cur_wq_month,
                 sites = unique(sites$field_site_id),
                 path = avro_file_directory)

# Download any new files from the Google Cloud
download.neon.avro(months = new_month_wq, 
                   sites = unique(sites$field_site_id), 
                   data_product = '20053',  # WQ data product
                   path = avro_file_directory)

# Read in the new files to append to the NEONstore data
# connect to spark locally 
sc <- sparklyr::spark_connect(master = "local")

# The variables (term names that should be kept)
prt_vars <- c('siteName',
              'startDate',
              'surfWaterTempMean',
              'surfWaterTempExpUncert', 
              'finalQF')

# Generate a list of files to be read
prt_avro_files <- paste0(avro_file_directory, '/',
                         list.files(path = avro_file_directory,
                                    pattern = '*20053', 
                                    recursive = T))

# Read in each of the files and then bind by rows
temp_streams_avros <- purrr::map_dfr(.x = prt_avro_files, ~ read.avro.prt(sc= sc, path = .x)) %>%
  QC.temp(df = ., range = c(-5, 40), spike = 7, by.depth = F) %>%
  mutate(measure_error = ifelse(is.na(observed), NA, measure_error))


  
#===============================================#

message("##### River temperature ######")
# For non-wadeable rivers need portal, EDI and avro data
  
  # Portal data
temp_rivers_portal <- arrow::open_dataset(neon$path("TSD_30_min-basic-DP1.20264.001")) %>%
  # neonstore::neon_table("TSD_30_min", site = sites$field_site_id) %>%
  rename(site_id = siteID,
         depth = thermistorDepth) %>%
  filter(site_id %in% nonwadable_rivers) %>%
  dplyr::select(startDateTime, site_id, tsdWaterTempMean, depth, tsdWaterTempExpUncert, tsdWaterTempFinalQF) %>%
  filter(tsdWaterTempFinalQF == 0) %>%
  dplyr::collect()

temp_rivers_portal <- temp_rivers_portal %>%
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(time, site_id) %>%
  dplyr::summarize(temperature__observed = mean(tsdWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(tsdWaterTempMean)),
                   temperature__sample_error = se(tsdWaterTempMean),
                   temperature__measure_error = mean(tsdWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  dplyr::select(time, site_id, temperature__observed, 
                temperature__sample_error, temperature__measure_error) %>%
  pivot_longer(cols = !c(time, site_id), 
               names_to = c("variable", "stat"),
               names_sep = '__') %>%
  pivot_wider(names_from = stat, values_from = value) 

temp_rivers_portal_QC <- temp_rivers_portal %>%
  QC.temp(df = ., range = c(-5, 40), spike = 7, by.depth = F)  %>%
  mutate(measure_error = ifelse(is.na(observed), NA, measure_error))

   # EDI data
edi_url_river <- c("https://pasta.lternet.edu/package/data/eml/edi/1185/1/fb9cf9ba62ee8e8cf94cb020175e9165",
                   "https://pasta.lternet.edu/package/data/eml/edi/1185/1/fac068cff680ae28473c3e13dc75aa9f",
                   "https://pasta.lternet.edu/package/data/eml/edi/1185/1/5567ad7252b598ee40f5653e7b732ff4" )

river_edi_profile <- c("NEON.D03.FLNT.DP0.20005.001.01378.csv",
                       "NEON.D03.BLWA.DP0.20005.001.01378.csv",
                       "NEON.D03.TOMB.DP0.20005.001.01378.csv")

for(i in 1:length(edi_url_river)){
  if (!file.exists(file.path(EDI_file_directory,river_edi_profile[i]))) {
    if (!dir.exists(dirname(file.path(EDI_file_directory, 
                                      river_edi_profile[i])))) {
      dir.create(dirname(file.path(EDI_file_directory, 
                                   river_edi_profile[i])))
    }
    download.file(edi_url_river[i], destfile = file.path(EDI_file_directory, river_edi_profile[i]))
  }
}

edi_data <- list.files(file.path(EDI_file_directory), full.names = T)

edi_rivers <- c(edi_data[grepl(x = edi_data, pattern= nonwadable_rivers[1])],
                edi_data[grepl(x = edi_data, pattern= nonwadable_rivers[2])],
                edi_data[grepl(x = edi_data, pattern= nonwadable_rivers[3])])

# The hourly data set is for the whole water column.
temp_rivers_EDI <- purrr::map_dfr(.x = edi_rivers, ~ read.csv(file = .x)) %>%
  rename('site_id' = siteID,
         'observed' = waterTemp) %>%
  mutate(startDate  = lubridate::ymd_hm(startDate),
         time = as.Date(startDate)) %>% 
  group_by(site_id, time) %>%
  summarise(temperature__observed = mean(observed),
            temperature__sample_error = se(observed)) %>%
  pivot_longer(names_to = c('variable','stat'),
               names_sep = '__',
               cols = c(temperature__observed,temperature__sample_error)) %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  mutate(measure_error = NA) %>%
  # include first QC of data
  QC.temp(df = ., range = c(-5, 40), spike = 5, by.depth = F)


  # avros

message("Generate a list of nonwadable_rivers avro files to be read")
tsd_avro_files <- paste0(avro_file_directory, '/',
                         list.files(path = avro_file_directory,
                                    pattern = '*20264', 
                                    recursive = T))
river_avro_files <- c(tsd_avro_files[grepl(x = tsd_avro_files, pattern= nonwadable_rivers[1])],
                      tsd_avro_files[grepl(x = tsd_avro_files, pattern= nonwadable_rivers[2])],
                      tsd_avro_files[grepl(x = tsd_avro_files, pattern= nonwadable_rivers[3])])

# Read in each of the files and then bind by rows
temp_rivers_avros <- purrr::map_dfr(.x = river_avro_files,
                                           ~ read.avro.tsd(sc= sc,
                                                           path = .x,
                                                           thermistor_depths = thermistor_depths)) %>% 
  # include first QC of data
  QC.temp(df = ., range = c(-5, 40), spike = 5, by.depth = F)  %>%
  mutate(measure_error = ifelse(is.na(observed), NA, measure_error))


spark_disconnect(sc)
#===========================================#

message("#### surface temperatures ####")

# Combine the avro files with the portal data
temp_full <- dplyr::bind_rows(# Lakes surface temperature
                              daily_temp_surface_lakes,
                              
                              # Stream temperature data
                              temp_streams_portal_QC,
                              temp_streams_avros,
                              
                              # River temperature data
                              temp_rivers_portal,
                              temp_rivers_EDI,
                              temp_rivers_avros) %>%
  dplyr::arrange(site_id, time) %>%
  group_by(site_id, time) %>%
  summarise(observed = mean(observed, na.rm = T),
            sample_error = mean(sample_error, na.rm = T),
            measure_error = mean(measure_error, na.rm = T)) %>%
  mutate(variable = 'temperature')


#### Temp QC protocol=================

# additional QC steps implemented (FO, 2022-07-13)
##### check 1 Gross range tests on temperature
# temperature ranges 
T_max <- 32 # gross max
T_min <- -2 # gross min

# GR flag will be true if the temperature is outside the range specified 
temp_cleaned <-
  temp_full %>%
  dplyr::mutate(observed =ifelse(observed >= T_min & observed <= T_max , 
                                    observed, NA),
                sample_error = ifelse(observed >= T_min & observed <= T_max , 
                            sample_error, NA),
                measure_error = ifelse(observed >= T_min & observed <= T_max , 
                               measure_error, NA))  %>%
  # manual cleaning based on observed
  dplyr:: mutate(observed = ifelse(site_id == "PRLA" & time <ymd("2019-01-01"),
                                      NA, observed),
                 sample_error = ifelse(site_id == "PRLA" & time <ymd("2019-01-01"),
                             NA, sample_error),
                 measure_error = ifelse(site_id == "PRLA" & time <ymd("2019-01-01"),
                                NA, measure_error))
 

#### Targets==========================
targets_long <- dplyr::bind_rows(wq_cleaned, temp_cleaned) %>%
  dplyr::arrange(site_id, time, variable) %>%
  dplyr::mutate(observed = ifelse(is.nan(observed), NA, observed),
                sample_error = ifelse(is.nan(sample_error), NA, sample_error),
                measure_error = ifelse(is.nan(measure_error), NA, measure_error))

### Write out the targets
write_csv(targets_long, "aquatics-targets.csv.gz")

### Write the disaggregated lake data
write_csv(hourly_temp_profile_lakes, "aquatics-expanded-observations.csv.gz")

readRenviron("~/.Renviron") # compatible with littler
## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../challenge-ci/R/publish.R")
publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-targets.csv.gz",
        prefix = "aquatics/",
        bucket = "neon4cast-targets",
        provdb = "prov.tsv",
        registries = "https://hash-archive.carlboettiger.info")

publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-expanded-observations.csv.gz",
        prefix = "aquatics/",
        bucket = "neon4cast-targets",
        provdb = "prov.tsv",
        registries = "https://hash-archive.carlboettiger.info")

system2("curl", "https://hc-ping.com/1267b13e-8980-4ddf-8aaa-21aa7e15081c")

message(paste0("Completed Aquatics Target at ", Sys.time()))
