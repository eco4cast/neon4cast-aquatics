message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)
library(contentid)

message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

message("neon_store(table = 'waq_instantaneous')")
neonstore::neon_store(table = "waq_instantaneous", n = 50)
message("neon_store(table = 'TSD_30_min')")
neonstore::neon_store(table = "TSD_30_min")
message("neon_store(table = 'TSW_30min')")
neonstore::neon_store(table = "TSW_30min")

## Load data from raw files
focal_sites <- c("BARC","POSE")
message("neon_table(table = 'waq_instantaneous')")
oxy <- neonstore::neon_table(table = "waq_instantaneous", site = focal_sites)
message("neon_table(table = 'TSD_30_min')")
temp_bouy <- neonstore::neon_table("TSD_30_min", site = focal_sites)
message("neon_table(table = 'TSW_30min')")
temp_prt <- neonstore::neon_table("TSW_30min", site = focal_sites) # not availaable for BARC 

#### Generate oxygen table #############

oxy_cleaned <- oxy %>%
  dplyr::select(siteID, startDateTime, sensorDepth, dissolvedOxygen,
                dissolvedOxygenExpUncert,dissolvedOxygenFinalQF) %>%
  dplyr::mutate(sensorDepth = as.numeric(sensorDepth),
                dissolvedOxygen = as.numeric(dissolvedOxygen),
                dissolvedOxygenExpUncert = as.numeric(dissolvedOxygenExpUncert)) %>% 
  dplyr::filter(dissolvedOxygenFinalQF == 0,
                (sensorDepth > 0 | is.na(sensorDepth)))%>%
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(time = as_date(startDateTime)) %>%
  dplyr::group_by(siteID, time) %>%
  dplyr::summarize(oxygen = mean(dissolvedOxygen, na.rm = TRUE),
                   sensorDepth = mean(sensorDepth, na.rm = TRUE),
                   count = sum(!is.na(dissolvedOxygen)),
                   oxygen_sd = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
  dplyr::filter(count > 44) %>% 
  dplyr::select(time, siteID, sensorDepth, oxygen, oxygen_sd) %>% 
  dplyr::rename(depth_oxygen = sensorDepth) %>% 
  mutate(neon_product_id = 'DP1.20288.001')


### Generate surface (< 2 m) temperature #############

temp_bouy_cleaned <- temp_bouy %>%
  dplyr::select(startDateTime, siteID, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert, tsdWaterTempFinalQF) %>%
  dplyr::filter(thermistorDepth > 0.75 & thermistorDepth < 1.25 & tsdWaterTempFinalQF == 0) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>%
  dplyr::group_by(time, siteID, thermistorDepth) %>%
  dplyr::summarize(thermistorDepth = mean(thermistorDepth, na.rm = TRUE),
                   temperature = mean(tsdWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(tsdWaterTempMean)),
                   temperature_sd = mean(tsdWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  dplyr::filter(count > 44) %>%  
  dplyr::rename(depth_temperature = thermistorDepth) %>% 
  dplyr::select(time, siteID, depth_temperature, temperature, temperature_sd) %>% 
  mutate(neon_product_id = 'DP1.20264.001')

temp_prt_cleaned <- temp_prt %>%
  dplyr::select(startDateTime, siteID, surfWaterTempMean, surfWaterTempExpUncert, finalQF) %>%
  dplyr::filter(finalQF == 0) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(time, siteID) %>%
  dplyr::summarize(temperature = mean(surfWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(surfWaterTempMean)),
                   temperature_sd = mean(surfWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  dplyr::filter(count > 44) %>% 
  dplyr::mutate(depth_temperature = NA) %>% 
  dplyr::select(time, siteID, depth_temperature, temperature, temperature_sd) %>% 
  mutate(neon_product_id = 'DP1.20053.001')

temp_cleaned <- rbind(temp_bouy_cleaned, temp_prt_cleaned)

targets <- full_join(oxy_cleaned, temp_cleaned, by = c("time","siteID")) %>% 
  select(time, siteID, oxygen, temperature, oxygen_sd, temperature_sd, depth_oxygen, depth_temperature, starts_with('neon_product_id')) %>% 
  mutate(neon_product_ids = paste(neon_product_id.x, neon_product_id.y, sep = '; ')) %>% 
  select(-neon_product_id.x, -neon_product_id.y)

targets %>% 
  select(time, siteID, oxygen, temperature) %>% 
  pivot_longer(-c("time","siteID"), names_to = "variable", values_to = "value") %>% 
  ggplot(aes(x = time, y = value)) +
  geom_point() +
  facet_wrap(siteID~variable)


### Write out the targets

write_csv(targets, "aquatics-targets.csv.gz")

## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../neon4cast-shared-utilities/publish.R")
publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-targets.csv.gz",
        prefix = "aquatics/",
        bucket = "targets")

message(paste0("Completed Aquatics Target at ", Sys.time()))
