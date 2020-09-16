## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)

## Load data from raw files
focal_sites <- c("BARC","FLNT")
oxy <- neonstore::neon_read(table = "waq_instantaneous", site = focal_sites)
temp <- neon_read("TSD_30_min", site = focal_sites)


#### Generate oxygen table #############

oxy_cleaned <- oxy %>%
  dplyr::select(siteID, startDateTime, sensorDepth, dissolvedOxygen,
                dissolvedOxygenExpUncert,dissolvedOxygenFinalQF) %>%
  dplyr::filter(dissolvedOxygenFinalQF == 0,
                sensorDepth > 0) %>%
  dplyr::mutate(startDateTime = as_datetime(startDateTime)) %>%
  dplyr::mutate(date = as_date(startDateTime),
                hour = hour(startDateTime)) %>%
  dplyr::group_by(siteID, date, hour) %>%
  dplyr::summarize(sensorDepth = mean(sensorDepth, na.rm = TRUE),
                   dissolvedOxygen = mean(dissolvedOxygen, na.rm = TRUE),
                   dissolvedOxygenExpUncert = mean(dissolvedOxygenExpUncert, na.rm = TRUE),
                   sensorDepth = mean(sensorDepth, na.rm = TRUE), .groups = "drop") %>%
  dplyr::mutate(startDateTime = make_datetime(year = year(date), month = month(date),
                                              day = day(date), hour = hour,
                                              min = 0, tz ="UTC")) %>%
  dplyr::select(siteID, startDateTime, sensorDepth, dissolvedOxygen, dissolvedOxygenExpUncert)

### Generate surface (< 2 m) temperature #############

temp_cleaned <- temp %>%
  dplyr::select(startDateTime, siteID, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert) %>%
  dplyr::mutate(date = as_date(startDateTime),
                hour = hour(startDateTime)) %>%
  dplyr::group_by(date, siteID, hour,thermistorDepth) %>%
  dplyr::summarize(tsdWaterTempMean = mean(tsdWaterTempMean, na.rm = TRUE),
                   tsdWaterTempExpUncert = mean(tsdWaterTempExpUncert, na.rm = TRUE), .groups = "drop") %>%
  dplyr::mutate(startDateTime = make_datetime(year = year(date), month = month(date),
                                              day = day(date), hour = hour, min = 0,
                                              tz ="UTC")) %>%
  dplyr::select(startDateTime, siteID, tsdWaterTempMean,thermistorDepth,tsdWaterTempExpUncert) %>%
  dplyr::group_by(startDateTime, siteID, thermistorDepth) %>%
  dplyr::summarise(tsdWaterTempMean = mean(tsdWaterTempMean, na.rm = TRUE),
                   tsdWaterTempExpUncert = mean(tsdWaterTempExpUncert), .groups = "drop") %>%
  dplyr::filter(thermistorDepth == min(thermistorDepth))

### Combine oxygen and temperature together into a single table

temp_targets <- temp_cleaned %>%
  rename(time = startDateTime,
         water_temperature = tsdWaterTempMean,
         water_temperature_sd = tsdWaterTempExpUncert,
         depth = thermistorDepth)

oxygen_targets <- oxy_cleaned %>%
  rename(time = startDateTime,
         dissolved_oxygen = dissolvedOxygen,
         dissolved_oxygen_sd = dissolvedOxygenExpUncert,
         depth = sensorDepth)

time <- tibble(time = seq(min(temp_targets$time),max(temp_targets$time), by = "1 hour"))

### Create a time vector without time gaps and join with the aquatics table to
### continusoul time vector

temp_targets <- left_join(time, temp_targets, by = "time") %>%
  select(time, siteID, water_temperature, water_temperature_sd, depth) %>%
  mutate(water_temperature = ifelse(is.nan(water_temperature), NA, water_temperature),
         water_temperature_sd = ifelse(is.nan(water_temperature_sd), NA, water_temperature_sd),
         depth = ifelse(is.nan(depth), NA, depth))

oxygen_targets <- left_join(time, oxygen_targets, by = "time") %>%
  select(time, siteID, dissolved_oxygen, dissolved_oxygen_sd, depth) %>%
  mutate(dissolved_oxygen = ifelse(is.nan(dissolved_oxygen), NA, dissolved_oxygen),
         dissolved_oxygen_sd = ifelse(is.nan(dissolved_oxygen_sd), NA, dissolved_oxygen_sd),
         depth = ifelse(is.nan(depth), NA, depth))

### Write out the targets

write_csv(temp_targets, "aquatic-temperature-targets.csv.gz")
write_csv(oxygen_targets, "aquatic-oxygen-targets.csv.gz")

## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../NEON-community-forecast/R/publish.R")
publish(code = c("02_generate_targets_aquatics.R"),
        data_out = c("aquatic-temperature-targets.csv.gz","aquatic-oxygen-targets.csv.gz"),
        prefix = "aquatics/",
        bucket = "targets")
