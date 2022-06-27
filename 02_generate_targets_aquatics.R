message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

Sys.setenv("NEONSTORE_HOME" = "/efi_neon_challenge/neonstore")
Sys.setenv("NEONSTORE_DB" = "/efi_neon_challenge/neonstore")

## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)
library(contentid)

message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-aquatics/master/Aquatic_NEON_Field_Site_Metadata_20210928.csv")


focal_sites <- sites$field_site_id
additional_sites <- c("SUGG", "PRLA", "PRPO", "LIRO")

message("Downloading: DP1.20288.001")
neonstore::neon_download("DP1.20288.001",site = c(focal_sites,additional_sites), type = "basic")
neonstore::neon_store(table = "waq_instantaneous", n = 50)
message("Downloading: DP1.20264.001")
neonstore::neon_download("DP1.20264.001", site =  c(focal_sites,additional_sites), type = "basic")
neonstore::neon_store(table = "TSD_30_min")
message("Downloading: DP1.20053.001")
neonstore::neon_download("DP1.20053.001", site =  c(focal_sites,additional_sites), type = "basic")
neonstore::neon_store(table = "TSW_30min")

## Load data from raw files
message("neon_table(table = 'waq_instantaneous')")
wq_raw <- neonstore::neon_table(table = "waq_instantaneous", site = c(focal_sites,additional_sites))
message("neon_table(table = 'TSD_30_min')")
temp_bouy <- neonstore::neon_table("TSD_30_min", site = c(focal_sites,additional_sites))
message("neon_table(table = 'TSW_30min')")
temp_prt <- neonstore::neon_table("TSW_30min", site = c(focal_sites,additional_sites)) 

#### Generate oxygen table #############

wq_cleaned <- wq_raw %>%
  dplyr::select(siteID, startDateTime, sensorDepth, dissolvedOxygen,
                dissolvedOxygenExpUncert,dissolvedOxygenFinalQF,
                chlorophyll,
                chlorophyllExpUncert,chlorophyllFinalQF) %>%
  dplyr::mutate(sensorDepth = as.numeric(sensorDepth),
                dissolvedOxygen = as.numeric(dissolvedOxygen),
                dissolvedOxygenExpUncert = as.numeric(dissolvedOxygenExpUncert),
                chla = as.numeric(chlorophyll),
                chlorophyllExpUncert = as.numeric(chlorophyllExpUncert)) %>% 
  
  # sensor depth of NA == surface?
  dplyr::filter(((sensorDepth > 0 & sensorDepth < 1)| is.na(sensorDepth))) %>%
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(time = as_date(startDateTime)) %>%
  
  # QF (quality flag) == 0, is a pass (1 == fail), 
    # make NA so these values are not used in the mean summary
  dplyr::mutate(dissolvedOxygen = ifelse(dissolvedOxygenFinalQF == 0, dissolvedOxygen, NA),
                 chla = ifelse(chlorophyllFinalQF == 0, chla, NA)) %>% 
  dplyr::group_by(siteID, time) %>%
  dplyr::summarize(oxygen = mean(dissolvedOxygen, na.rm = TRUE),
                   sensorDepth = mean(sensorDepth, na.rm = TRUE),
                   chla = mean(chla, na.rm = TRUE),
                   
                   #why only using the count of non-NA in DO?
                   count = sum(!is.na(dissolvedOxygen)),
                   chla_sd = mean(chlorophyllExpUncert, na.rm = TRUE)/sqrt(count),
                   oxygen_sd = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::select(time, siteID, sensorDepth, oxygen, chla, oxygen_sd, chla_sd) %>% 
  
  #why only depth of DO not chl? all between 0 and 1 anyway
  dplyr::rename(depth_oxygen = sensorDepth)

wq_cleaned %>% 
  ggplot(aes(x = time, y = oxygen)) +
  geom_point() +
  facet_wrap(~siteID)

### Generate surface (< 1 m) temperature #############
  # "raw data" is the 30 min average taken from 1 min measurements


## lake temperatures ##
temp_bouy_cleaned <- temp_bouy %>%
  dplyr::select(startDateTime, siteID, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert, tsdWaterTempFinalQF, verticalPosition) %>%
  # errors in the sensor depths reported - see "https://www.neonscience.org/impact/observatory-blog/incorrect-depths-associated-lake-and-river-temperature-profiles"
    # sensor depths are manually assigned based on "vertical position" variable as per table on webpage
  mutate(thermistorDepth = ifelse(siteID == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 502, 1.75, thermistorDepth),
         thermistorDepth = ifelse(siteID == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 503, 3.45, thermistorDepth),
         thermistorDepth = ifelse(siteID == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 504, 5.15, thermistorDepth),
         thermistorDepth = ifelse(siteID == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 505, 6.85, thermistorDepth),
         thermistorDepth = ifelse(siteID == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 506, 8.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "CRAM" & 
                                    lubridate::as_date(startDateTime) < lubridate::as_date("2020-11-01") & 
                                    verticalPosition == 505, 10.25, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 502, 0.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 503, 1.05, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 504, 1.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 505, 2.05, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 506, 2.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) < lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) > lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 507, 3.05, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 502, 0.3, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 503, 0.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 504, 0.8, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 505, 1.05, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 506, 1.3, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 507, 1.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 508, 2.05, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") & 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 509, 2.55, thermistorDepth),
         thermistorDepth = ifelse(siteID == "BARC" & 
                                    (lubridate::as_date(startDateTime) >= lubridate::as_date("2021-06-08") | 
                                       lubridate::as_date(startDateTime) <= lubridate::as_date("2022-01-07") )& 
                                    verticalPosition == 510, 3.05, thermistorDepth)) %>% 
  # take only surface measurements with no flags 
    # OR if there is a flag but the date is within the period defined by the issues on the sensorDepth above
  dplyr::filter(thermistorDepth <= 1.0 & (tsdWaterTempFinalQF == 0 | (tsdWaterTempFinalQF == 1 & as_date(startDateTime) > as_date("2020-07-01")))) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>%
  dplyr::group_by(time, siteID, thermistorDepth) %>%
  dplyr::summarize(thermistorDepth = mean(thermistorDepth, na.rm = TRUE),
                   temperature = mean(tsdWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(tsdWaterTempMean)),
                   temperature_sd = mean(tsdWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>%  
  dplyr::rename(depth_temperature = thermistorDepth) %>% 
  dplyr::select(time, siteID, depth_temperature, temperature, temperature_sd) 

 

## river temperatures ##
temp_prt_cleaned <- temp_prt %>%
  # horizontal position is upstream or downstream is 101 or 102 horizontalposition
  dplyr::filter(horizontalPosition == "102") %>% 
  dplyr::select(startDateTime, siteID, surfWaterTempMean, surfWaterTempExpUncert, finalQF) %>%
  dplyr::filter(finalQF == 0) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(time, siteID) %>%
  dplyr::summarize(temperature = mean(surfWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(surfWaterTempMean)),
                   temperature_sd = mean(surfWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::mutate(depth_temperature = NA) %>% 
  dplyr::select(time, siteID, depth_temperature, temperature, temperature_sd)


temp_cleaned <- rbind(temp_bouy_cleaned, temp_prt_cleaned)

targets <- full_join(wq_cleaned, temp_cleaned, by = c("time","siteID")) %>% 
  select(time, siteID, oxygen, temperature, chla, oxygen_sd, temperature_sd, chla_sd, depth_oxygen, depth_temperature)

targets %>% 
  select(time, siteID, oxygen, temperature, chla) %>% 
  pivot_longer(-c("time","siteID"), names_to = "variable", values_to = "value") %>% 
  ggplot(aes(x = time, y = value)) +
  geom_point() +
  facet_grid(variable~siteID, scales = "free")



### Write out the targets

write_csv(targets, "aquatics-targets.csv.gz")

## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../neon4cast-shared-utilities/publish.R")
publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-targets.csv.gz",
        prefix = "aquatics/",
        bucket = "targets",
        provdb = "prov.tsv",
        registries = "https://hash-archive.carlboettiger.info")

message(paste0("Completed Aquatics Target at ", Sys.time()))
