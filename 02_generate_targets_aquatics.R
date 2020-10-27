print(paste0("Running Creating Aquatics Targets at ", Sys.time()))

renv::restore()

## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)
library(contentid)

print(paste0("Running Creating Aquatics Targets at ", Sys.time()))

neonstore::neon_store(table = "waq_instantaneous")
neonstore::neon_store(table = "TSD_30_min")

## Load data from raw files
focal_sites <- c("BARC","FLNT")
oxy <- neonstore::neon_read(table = "waq_instantaneous", site = focal_sites)
print(neonstore::neon_dir())
print(getwd())
temp <- neonstore::neon_read("TSD_30_min", site = focal_sites)


#### Generate oxygen table #############

oxy_cleaned <- oxy %>%
  dplyr::select(siteID, startDateTime, sensorDepth, dissolvedOxygen,
                dissolvedOxygenExpUncert,dissolvedOxygenFinalQF) %>%
  dplyr::filter(dissolvedOxygenFinalQF == 0,
                sensorDepth > 0) %>%
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(siteID, time) %>%
  dplyr::summarize(oxygen = mean(dissolvedOxygen, na.rm = TRUE),
                   oxygen_sd = mean(dissolvedOxygenExpUncert, na.rm = TRUE),
                   sensorDepth = mean(sensorDepth, na.rm = TRUE), .groups = "drop") %>%
  dplyr::select(siteID, time, sensorDepth, oxygen, oxygen_sd) %>% 
  dplyr::rename(depth_oxygen = sensorDepth)

### Generate surface (< 2 m) temperature #############

temp_cleaned <- temp %>%
  dplyr::select(startDateTime, siteID, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert, tsdWaterTempFinalQF) %>%
  dplyr::filter(thermistorDepth > 0.75 & thermistorDepth < 1.25 & tsdWaterTempFinalQF == 0) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>%
  dplyr::group_by(time, siteID, thermistorDepth) %>%
  dplyr::summarize(thermistorDepth = mean(thermistorDepth, na.rm = TRUE),
                   temperature = mean(tsdWaterTempMean, na.rm = TRUE),
                   temperature_sd = mean(tsdWaterTempExpUncert, na.rm = TRUE), .groups = "drop") %>% 
  dplyr::rename(depth_temperature = thermistorDepth)

targets <- full_join(oxy_cleaned, temp_cleaned, by = c("time","siteID")) %>% 
  select(time, siteID, oxygen, temperature, oxygen_sd, temperature_sd, depth_oxygen, depth_temperature)

targets %>% 
  filter(siteID == "BARC") %>% 
  select(time, oxygen, temperature) %>% 
  pivot_longer(-time, names_to = "variable", values_to = "value") %>% 
  ggplot(aes(x = time, y = value)) +
  geom_point() +
  facet_wrap(~variable)


### Write out the targets

write_csv(targets, "aquatics-targets.csv.gz")

## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../neon4cast-shared-utilities/publish.R")
publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-targets.csv.gz",
        prefix = "aquatics/",
        bucket = "targets")

print(paste0("Completed Aquatics Target at ", Sys.time()))
