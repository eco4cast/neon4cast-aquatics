message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

avro_file_directory <- "/home/rstudio/data/aquatic_avro"
Sys.setenv("NEONSTORE_HOME" = "/home/rstudio/data/neonstore") #Sys.setenv("NEONSTORE_HOME" = "/efi_neon_challenge/neonstore")
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
# spark_install(version = '3.0')

`%!in%` <- Negate(`%in%`) # not in function

message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

sites <- read_csv("https://raw.githubusercontent.com/OlssonF/neon4cast-aquatics/master/Aquatic_NEON_Field_Site_Metadata_20220727.csv")

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


#### Generate WQ table #############
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
  dplyr::collect()
wq_portal <- wq_portal %>% # sensor depth of NA == surface?
  dplyr::filter(((sensorDepth > 0 & sensorDepth < 1)| is.na(sensorDepth))) %>%
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(time = as_date(startDateTime)) %>%
  
  # QF (quality flag) == 0, is a pass (1 == fail), 
  # make NA so these values are not used in the mean summary
  dplyr::mutate(dissolvedOxygen = ifelse(dissolvedOxygenFinalQF == 0, dissolvedOxygen, NA),
                chla = ifelse(chlorophyllFinalQF == 0, chla, NA)) %>% 
  dplyr::group_by(site_id, time) %>%
  dplyr::summarize(oxygen_observation = mean(dissolvedOxygen, na.rm = TRUE),
                   sensorDepth = mean(sensorDepth, na.rm = TRUE),
                   chla_observation = mean(chla, na.rm = TRUE),
                   
                   oxygen_sample.sd = sd(dissolvedOxygen, na.rm = TRUE),
                   chla_sample.sd = sd(chla, na.rm = TRUE),
                   #why only using the count of non-NA in DO?
                   count = sum(!is.na(dissolvedOxygen)),
                   chla_measure.error = mean(chlorophyllExpUncert, na.rm = TRUE)/sqrt(count),
                   oxygen_measure.error = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::select(time, site_id, oxygen_observation, chla_observation,oxygen_sample.sd, chla_sample.sd, oxygen_measure.error, chla_measure.error) %>% 
  
  
  
  pivot_longer(cols = !c(time, site_id), names_to = c("variable", "stat"), names_sep = "_") %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  filter(!(variable == "chla" & site_id %in% stream_sites))
  
#====================================================#

# ======= low latency WQ data =======
# download the 24/48hr provisional data from the Google Cloud

# where should these files be saved?

download_location <- avro_file_directory
fs::dir_create(download_location)

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
                 path = download_location)


# Download any new files from the Google Cloud
download.neon.avro(months = new_month_wq, 
                   sites = unique(wq_portal$site_id), #unique(sites$field_site_id), 
                   data_product = '20288',  # WQ data product
                   path = download_location)

# Read in the new files to append to the NEONstore data
# connect to spark locally 
sc <- sparklyr::spark_connect(master = "local", version = '3.0', )

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
wq_avro_files <- paste0(download_location, '/',
                        list.files(path = paste0(download_location), 
                                   pattern = '*20288', 
                                   recursive = T))

# Read in each of the files and then bind by rows
wq_avro_df <- purrr::map_dfr(.x = wq_avro_files, ~ read.avro.wq(sc= sc, path = .x))


# Combine the avro files with the portal data
wq_full <- dplyr::bind_rows(wq_portal, wq_avro_df) %>%
  dplyr::arrange(site_id, time)


#==============================#

# ======== WQ QC protocol =======
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
  dplyr::mutate(observation = ifelse(is.na(observation),
                                     observation, ifelse(observation >= DO_min & observation <= DO_max & variable == 'oxygen', 
                                                         observation, ifelse(observation >= chla_min & observation <= chla_max & variable == 'chla', observation, NA)))) %>%
  # manual cleaning based on visual inspection
  dplyr::mutate(observation = ifelse(site_id == "MAYF" & 
                                       between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                                       variable == "oxygen", NA, observation),
                observation = ifelse(site_id == "WLOU" &
                                       !between(observation, 7.5, 11) & 
                                       variable == "oxygen", NA, observation),
                observation = ifelse(site_id == "BARC" & 
                                       observation < 4 &
                                       variable == "oxygen", NA, observation),
                
                measure.error = ifelse(site_id == "MAYF" &
                                 between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                                 variable == "oxygen", NA, measure.error),
                measure.error = ifelse(site_id == "WLOU" &
                                 !between(observation, 7.5, 11) & 
                                 variable == "oxygen", NA, measure.error),
                measure.error = ifelse(site_id == "BARC" & 
                                 observation < 4 &
                                 variable == "oxygen", NA, measure.error),
                
                sample.sd = ifelse(site_id == "MAYF" & 
                              between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                              variable == "oxygen", NA, sample.sd),
                sample.sd = ifelse(site_id == "WLOU" &
                              !between(observation, 7.5, 11) & 
                              variable == "oxygen", NA, sample.sd),
                sample.sd = ifelse(site_id == "BARC" & 
                              observation < 4 &
                              variable == "oxygen", NA, sample.sd))

#===============================================#


### Generate surface (< 1 m) temperature #############
  # "raw data" (L1 NEON data product) is the 30 min average taken from 1 min measurements

## lake temperatures ##
temp_buoy <- arrow::open_dataset(neon$path("TSD_30_min-basic-DP1.20264.001")) %>%
  # neonstore::neon_table("TSD_30_min", site = sites$field_site_id) %>%
  rename(site_id = siteID) %>%
  dplyr::select(startDateTime, site_id, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert, tsdWaterTempFinalQF, verticalPosition) %>%
  dplyr::collect()

temp_buoy <- temp_buoy %>%
  # errors in the sensor depths reported - see "https://www.neonscience.org/impact/observatory-blog/incorrect-depths-associated-lake-and-river-temperature-profiles"
    # sensor depths are manually assigned based on "vertical position" variable as per table on webpage
  dplyr::mutate(thermistorDepth = ifelse(site_id == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 502, 1.75, thermistorDepth),
         thermistorDepth = ifelse(site_id == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 503, 3.45, thermistorDepth),
         thermistorDepth = ifelse(site_id == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 504, 5.15, thermistorDepth),
         thermistorDepth = ifelse(site_id == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 505, 6.85, thermistorDepth),
         thermistorDepth = ifelse(site_id == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 506, 8.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 505, 10.25, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 502, 0.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 503, 1.05, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 504, 1.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 505, 2.05, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 506, 2.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 507, 3.05, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 502, 0.3, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 503, 0.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 504, 0.8, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 505, 1.05, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 506, 1.3, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 507, 1.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 508, 2.05, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 509, 2.55, thermistorDepth),
         thermistorDepth = ifelse(site_id == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 510, 3.05, thermistorDepth)) %>% 
  # take only surface measurements with no flags 
    # OR if there is a flag but the date is within the period defined by the issues on the sensorDepth above
  dplyr::filter(thermistorDepth <= 1.0 & (tsdWaterTempFinalQF == 0 | (tsdWaterTempFinalQF == 1 & as_date(startDateTime) > as_date("2020-07-01")))) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>%
  dplyr::group_by(time, site_id) %>% # use all the depths
  dplyr::summarize(temperature_observation = mean(tsdWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(tsdWaterTempMean)),
                   temperature_sample.sd = sd(tsdWaterTempMean, na.rm = T),
                   temperature_measure.error = mean(tsdWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>%  
  dplyr::select(time, site_id, temperature_observation, temperature_sample.sd, temperature_measure.error) 
 
## river temperatures ##
temp_prt <- arrow::open_dataset(neon$path("TSW_30min-basic-DP1.20053.001")) %>% 
  # neonstore::neon_table("TSW_30min", site = sites$field_site_id) %>%
  dplyr::rename(site_id = siteID) %>%
  # horizontal position is upstream or downstream is 101 or 102 horizontal position
  dplyr::filter(horizontalPosition == "101") %>%  # take upstream to match WQ data
  dplyr::select(startDateTime, site_id, surfWaterTempMean, surfWaterTempExpUncert, finalQF) %>%
  dplyr::filter(finalQF == 0) %>%
  dplyr::collect()

temp_prt <- temp_prt %>%
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(time, site_id) %>%
  dplyr::summarize(temperature_observation = mean(surfWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(surfWaterTempMean)),
                   temperature_sample.sd = sd(surfWaterTempMean),
                   temperature_measure.error = mean(surfWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::select(time, site_id, temperature_observation, temperature_sample.sd, temperature_measure.error) 

temp_tsd_prt <- rbind(temp_buoy, temp_prt) %>%
  pivot_longer(cols = !c(time, site_id), names_to = c("variable", "stat"), names_sep = "_") %>%
  pivot_wider(names_from = stat, values_from = value)


#========= low latency water temperature data =========
# download the 24/48hr provisional data from the Google Cloud

# Start by deleting superseded files
# Files that have been supersed by the NEON store files can be deleted from the relevent repository
# Look in each repository to see if there are files that match the current maximum month of the NEON
# store data

delete.neon.avro(months = cur_wq_month,
                 sites = unique(temp_bouy$site_id),# unique(sites$field_site_id),
                 path = download_location)


# Download any new files from the Google Cloud
download.neon.avro(months = new_month_wq, 
                   sites = unique(sites$field_site_id), 
                   data_product = '20264',  # WQ data product
                   path = download_location)

download.neon.avro(months = new_month_wq, 
                   sites = unique(sites$field_site_id), 
                   data_product = '20053',  # WQ data product
                   path = download_location)

# Read in the new files to append to the NEONstore data
# connect to spark locally 
sc <- sparklyr::spark_connect(master = "local")

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
tsd_avro_files <- paste0(download_location, '/',
                         list.files(path = download_location,
                                    pattern = '*20264', 
                                    recursive = T))

# Read in each of the files and then bind by rows
tsd_avro_df <- purrr::map_dfr(.x = tsd_avro_files, ~ read.avro.tsd(sc= sc, path = .x, thermistor_depths = thermistor_depths))


# The variables (term names that should be kept)
prt_vars <- c('siteName',
              'startDate',
              'surfWaterTempMean',
              'surfWaterTempExpUncert', 
              'finalQF')

# Generate a list of files to be read
prt_avro_files <- paste0(download_location, '/',
                         list.files(path = download_location,
                                    pattern = '*20053', 
                                    recursive = T))

# Read in each of the files and then bind by rows
prt_avro_df <- purrr::map_dfr(.x = prt_avro_files, ~ read.avro.prt(sc= sc, path = .x))


# Combine the avro files with the portal data
temp_full <- dplyr::bind_rows(temp_tsd_prt, tsd_avro_df, prt_avro_df) %>%
  dplyr::arrange(site_id, time)


#==============================#

# additional QC steps implemented (FO, 2022-07-13)
##### check 1 Gross range tests on temperature
# temperature ranges 
T_max <- 32 # gross max
T_min <- -2 # gross min

# GR flag will be true if the temperature is outside the range specified 
temp_cleaned <-
  temp_full %>%
  dplyr::mutate(observation =ifelse(observation >= T_min & observation <= T_max , 
                                    observation, NA),
                sample.sd = ifelse(observation >= T_min & observation <= T_max , 
                            sample.sd, NA),
                measure.error = ifelse(observation >= T_min & observation <= T_max , 
                               measure.error, NA))  %>%
  # manual cleaning based on observationervations
  dplyr:: mutate(observation = ifelse(site_id == "PRLA" & time <ymd("2019-01-01"),
                                      NA, observation),
                 sample.sd = ifelse(site_id == "PRLA" & time <ymd("2019-01-01"),
                             NA, sample.sd),
                 measure.error = ifelse(site_id == "PRLA" & time <ymd("2019-01-01"),
                                NA, measure.error))
 

#==============================================
targets_long <- dplyr::bind_rows(wq_cleaned, temp_cleaned) %>%
  dplyr::arrange(site_id, time, variable) %>%
  dplyr::mutate(observation = ifelse(is.nan(observation), NA, observation),
                sample.sd = ifelse(is.nan(sample.sd), NA, sample.sd),
                measure.error = ifelse(is.nan(measure.error), NA, measure.error))

### Write out the targets
write_csv(targets_long, "aquatics-targets.csv.gz")

readRenviron("~/.Renviron") # compatible with littler
## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../challenge-ci/publish.R")
publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-targets.csv.gz",
        prefix = "aquatics/",
        bucket = "neon4cast-targets",
        provdb = "prov.tsv",
        registries = "https://hash-archive.carlboettiger.info")

message(paste0("Completed Aquatics Target at ", Sys.time()))
