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

## Load data from raw files
message("neon_table(table = 'waq_instantaneous')")
wq_raw <- neonstore::neon_table(table = "waq_instantaneous", site = focal_sites)
message("neon_table(table = 'TSD_30_min')")
temp_bouy <- neonstore::neon_table("TSD_30_min", site = focal_sites)
message("neon_table(table = 'TSW_30min')")
temp_prt <- neonstore::neon_table("TSW_30min", site = focal_sites) 

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
  dplyr::filter(((sensorDepth > 0 & sensorDepth < 1)| is.na(sensorDepth))) %>%
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(time = as_date(startDateTime)) %>%
  dplyr::mutate(dissolvedOxygen = ifelse(dissolvedOxygenFinalQF == 0, dissolvedOxygen, NA),
                 chla = ifelse(chlorophyllFinalQF == 0, chla, NA)) %>% 
  dplyr::group_by(siteID, time) %>%
  dplyr::summarize(oxygen = mean(dissolvedOxygen, na.rm = TRUE),
                   sensorDepth = mean(sensorDepth, na.rm = TRUE),
                   chla = mean(chla, na.rm = TRUE),
                   count = sum(!is.na(dissolvedOxygen)),
                   chla_sd = mean(chlorophyllExpUncert, na.rm = TRUE)/sqrt(count),
                   oxygen_sd = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::select(time, siteID, sensorDepth, oxygen, chla, oxygen_sd, chla_sd) %>% 
  dplyr::rename(depth_oxygen = sensorDepth)

wq_cleaned %>% 
  ggplot(aes(x = time, y = chla)) +
  geom_point() +
  facet_wrap(~siteID)

### Generate surface (< 1 m) temperature #############

temp_bouy_cleaned <- temp_bouy %>%
  dplyr::select(startDateTime, siteID, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert, tsdWaterTempFinalQF) %>%
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

temp_prt_cleaned <- temp_prt %>%
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
  facet_grid(siteID~variable, scales = "free")


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
