## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)


## Load data from raw files
focal_sites <- c("BARC","FLNT")
oxy <- neon_read(table = "waq_instantaneous", site = focal_sites)
temp <- neon_read("TSD_30_min", site = focal_sites)


#### Generate oxygen table #############

oxy <- oxy %>%
  select(siteID, startDateTime, sensorDepth, dissolvedOxygen,
         dissolvedOxygenExpUncert,dissolvedOxygenFinalQF) %>%
  filter(dissolvedOxygenFinalQF == 0,
         sensorDepth > 0) %>%
  mutate(startDateTime = as_datetime(startDateTime))

oxy <- oxy %>%
  mutate(date = as_date(startDateTime),
         hour = hour(startDateTime)) %>%
  group_by(siteID, date, hour) %>%
  summarize(sensorDepth = mean(sensorDepth, na.rm = TRUE),
            dissolvedOxygen = mean(dissolvedOxygen, na.rm = TRUE),
            dissolvedOxygenExpUncert = mean(dissolvedOxygenExpUncert, na.rm = TRUE), .groups = "drop") %>%
  mutate(startDateTime = make_datetime(year = year(date), month = month(date),
                                       day = day(date), hour = hour,
                                       min = 0, tz ="UTC")) %>%
  select(siteID, startDateTime, sensorDepth, dissolvedOxygen, dissolvedOxygenExpUncert)

### Generate surface (< 2 m) temperature #############

temp <- temp %>%
  select(startDateTime, siteID, tsdWaterTempMean, thermistorDepth, tsdWaterTempExpUncert)
  mutate(date = as_date(startDateTime),
         hour = hour(startDateTime)) %>%
  group_by(date, siteID, hour,thermistorDepth) %>%
  summarize(tsdWaterTempMean = mean(tsdWaterTempMean, na.rm = TRUE),
            tsdWaterTempExpUncert = mean(tsdWaterTempExpUncert, na.rm = TRUE), .groups = "drop") %>%
  mutate(startDateTime = make_datetime(year = year(date), month = month(date),
                                       day = day(date), hour = hour, min = 0,
                                       tz ="UTC")) %>%
  select(startDateTime, siteID, tsdWaterTempMean,thermistorDepth,tsdWaterTempExpUncert) %>%
    filter(thermistorDepth <  2.00) %>%
    group_by(startDateTime, siteID) %>%
    summarise(tsdWaterTempMean = mean(tsdWaterTempMean, na.rm = TRUE),
              tsdWaterTempExpUncert = mean(tsdWaterTempExpUncert), .groups = "drop")

### Combine oxygen and temperature together into a single table

aquatic_targets <- full_join(temp_surface, oxy, by = c("startDateTime", "siteID")) %>%
  rename(time = startDateTime,
         water_temperature = tsdWaterTempMean,
         water_temperature_sd = tsdWaterTempExpUncert,
         dissolved_oxygen = dissolvedOxygen,
         dissolved_oxygen_sd = dissolvedOxygenExpUncert) %>%
  select(time, siteID, water_temperature, water_temperature_sd, dissolved_oxygen, dissolved_oxygen_sd)

### Create a time vector without time gaps and join with the aquatics table to
### continusoul time vector

time <- tibble(time = seq(min(aquatic_targets$time),max(aquatic_targets$time), by = "1 hour"))

aquatic_targets <- left_join(time, aquatic_targets, by = "time")%>%
  mutate(water_temperature = ifelse(is.nan(water_temperature), NA, water_temperature),
         water_temperature_sd = ifelse(is.nan(water_temperature_sd), NA, water_temperature_sd),
         dissolved_oxygen = ifelse(is.nan(dissolved_oxygen), NA, dissolved_oxygen),
         dissolved_oxygen_sd = ifelse(is.nan(dissolved_oxygen_sd), NA, dissolved_oxygen_sd))

### Write out the targets

write_csv(aquatic_targets, "aquatic-oxygen-temperature.csv.gz")


## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("R/publish.R")
publish(code = c("02_generate_targets_aquatics.R"),
        data_out = "aquatic-oxygen-temperature.csv.gz",
        prefix = "aquatics/",
        bucket = "targets")
