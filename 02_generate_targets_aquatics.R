message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

Sys.setenv("NEONSTORE_HOME" = "/efi_neon_challenge/neonstore")
Sys.setenv("NEONSTORE_DB" = "/efi_neon_challenge/neonstore")

## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)
library(contentid)
`%!in%` <- Negate(`%in%`) # not in function

message(paste0("Running Creating Aquatics Targets at ", Sys.time()))

sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-aquatics/master/Aquatic_NEON_Field_Site_Metadata_20210928.csv")
aquatic_sites <- read.csv("../Aquatic NEON sites.csv")[, c("field_site_id", "field_site_type", "field_site_subtype")]

original_sites <- sites$field_site_id
additional_lakes <- aquatic_sites$field_site_id[which(aquatic_sites$field_site_id %!in% original_sites & 
                                                        aquatic_sites$field_site_subtype == "Lake")]
nonwadable_rivers <- aquatic_sites$field_site_id[(which(aquatic_sites$field_site_subtype == "Non-wadeable River"))]
lake_sites <- aquatic_sites$field_site_id[(which(aquatic_sites$field_site_subtype == "Lake"))]
# selected sites based on data availability
stream_sites <- c("ARIK", "MAYF", "WLOU", "KING", "HOPB", "MCRA", "LECO", "WALK", "PRIN", "LEWI")
rivers_and_streams <- c(stream_sites, nonwadable_rivers)

current_sites <- c(original_sites, additional_lakes, nonwadable_rivers, stream_sites)
missing_sites <- aquatic_sites$field_site_id[-which(as.vector(aquatic_sites$field_site_id) %in% current_sites)]
#======================================================#

message("Downloading: DP1.20288.001")
neonstore::neon_download("DP1.20288.001",site = rivers_and_streams, type = "basic")
neonstore::neon_store(table = "waq_instantaneous", n = 50)
message("Downloading: DP1.20264.001")
neonstore::neon_download("DP1.20264.001", site =  rivers_and_streams, type = "basic")
neonstore::neon_store(table = "TSD_30_min")
message("Downloading: DP1.20053.001")
neonstore::neon_download("DP1.20053.001", site =  rivers_and_streams, type = "basic")
neonstore::neon_store(table = "TSW_30min")

## Load data from raw files
# message("neon_table(table = 'waq_instantaneous')")
# wq_raw <- neonstore::neon_table(table = "waq_instantaneous", site = stream_sites[1:6]) 
# message("neon_table(table = 'TSD_30_min')")
# temp_bouy <- neonstore::neon_table("TSD_30_min", site = nonwadable_rivers)
# message("neon_table(table = 'TSW_30min')")
# temp_prt <- neonstore::neon_table("TSW_30min", site = stream_sites) 

#### Generate oxygen table #############
for (i in 1:length(rivers_and_streams)) {
  wq_cleaned <- neonstore::neon_table(table = "waq_instantaneous", site = rivers_and_streams[i]) %>% #wq_raw %>% 
    dplyr::select(siteID, startDateTime, sensorDepth,
                  dissolvedOxygen,dissolvedOxygenExpUncert,dissolvedOxygenFinalQF, 
                  chlorophyll, chlorophyllExpUncert,chlorophyllFinalQF) %>%
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
    dplyr::summarize(oxygen_obs = mean(dissolvedOxygen, na.rm = TRUE),
                     sensorDepth = mean(sensorDepth, na.rm = TRUE),
                     chla_obs = mean(chla, na.rm = TRUE),
                     
                     #why only using the count of non-NA in DO?
                     count = sum(!is.na(dissolvedOxygen)),
                     chla_error = mean(chlorophyllExpUncert, na.rm = TRUE)/sqrt(count),
                     oxygen_error = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
    #dplyr::filter(count > 44) %>% 
    dplyr::select(time, siteID, oxygen_obs, chla_obs, oxygen_error, chla_error) %>% 
    
    
    
    pivot_longer(cols = !c(time, siteID), names_to = c("variable", "stat"), names_sep = "_") %>%
    pivot_wider(names_from = stat, values_from = value)
  
  # if its a stream site we don't want the chlorophyll
  if (unique(wq_cleaned$siteID) %in% stream_sites) {
     wq_cleaned <- wq_cleaned %>% filter(variable == "oxygen")
  }
  
  # make a new table and then add each new site onto this
  if (exists("wq_cleaned_full")) {
    wq_cleaned_full <- rbind(wq_cleaned_full, wq_cleaned)
  } else {
    wq_cleaned_full <- wq_cleaned
  }
  print(paste0(i, "/", length(rivers_and_streams)))
}


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

wq_cleaned_flagged <- wq_cleaned_full %>%
  mutate(obs = ifelse(is.na(obs),
                          obs, ifelse(obs >= DO_min & obs <= DO_max & variable == 'oxygen', 
                                    obs, ifelse(obs >= chla_min & obs <= chla_max & variable == 'chla', obs, NA)))) %>%
  # manual cleaning based on visual inspection
  mutate(obs = ifelse(siteID == "MAYF" & 
                        between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                        variable == "oxygen", NA, 
                      ifelse(siteID == "WLOU" &
                               !between(obs, 7.5, 11) &
                               variable == "oxygen", NA, 
                             ifelse(siteID == "BARC" & 
                                      obs < 4 &
                                      variable == "oxygen", NA, obs))),
         error = ifelse(siteID == "MAYF" &
                       between(time, ymd("2019-01-20"), ymd("2019-02-05")) &
                       variable == "oxygen", NA, 
                     ifelse(siteID == "WLOU" &
                              !between(obs, 7.5, 11) &
                              variable == "oxygen", NA, 
                            ifelse(siteID == "BARC" & 
                                     obs < 4 &
                                     variable == "oxygen", NA, error))),
         sd = NA)

#===============================================#
### Generate surface (< 1 m) temperature #############
  # "raw data" (L1 NEON data product) is the 30 min average taken from 1 min measurements

## lake temperatures ##
temp_bouy_cleaned <- neonstore::neon_table("TSD_30_min", site = rivers_and_streams) %>%
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
  dplyr::group_by(time, siteID) %>% # use all the depths
  dplyr::summarize(temperature = mean(tsdWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(tsdWaterTempMean)),
                   temperature_sd = sd(tsdWaterTempMean, na.rm = T),
                   temperature_error = mean(tsdWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>%  
  dplyr::select(time, siteID, temperature, temperature_sd, temperature_error) 

 
## river temperatures ##
temp_prt_cleaned <- neonstore::neon_table("TSW_30min", site = rivers_and_streams) %>%
  # horizontal position is upstream or downstream is 101 or 102 horizontal position
  dplyr::filter(horizontalPosition == "101") %>%  # take upstream to match WQ data
  dplyr::select(startDateTime, siteID, surfWaterTempMean, surfWaterTempExpUncert, finalQF) %>%
  dplyr::filter(finalQF == 0) %>% 
  dplyr::mutate(time = as_date(startDateTime)) %>% 
  dplyr::group_by(time, siteID) %>%
  dplyr::summarize(temperature = mean(surfWaterTempMean, na.rm = TRUE),
                   count = sum(!is.na(surfWaterTempMean)),
                   temperature_error = mean(surfWaterTempExpUncert, na.rm = TRUE) /sqrt(count),.groups = "drop") %>%
  #dplyr::filter(count > 44) %>% 
  dplyr::select(time, siteID, temperature, temperature_error) %>%
  dplyr::mutate(temperature_sd = NA) # add an empty sd col so that we can rbin with buoy temps


temp_cleaned <- rbind(temp_bouy_cleaned, temp_prt_cleaned)

temp_longer <- temp_cleaned %>%
  rename(temperature_obs = temperature) %>%
  pivot_longer(cols = !c(time, siteID), names_to = c("variable", "stat"), names_sep = "_") %>%
  pivot_wider(names_from = stat, values_from = value)

# additional QC steps implemented (FO, 2022-07-13)
##### check 1 Gross range tests on temperature
# temperature ranges 
T_max <- 32 # gross max
T_min <- -2 # gross min

# GR flag will be true if the temperature is outside the range specified 
temp_cleaned_flagged <-
  temp_longer %>%
  mutate(obs =ifelse(obs >= T_min & obs <= T_max , 
                                    obs, NA),
         sd = ifelse(obs >= T_min & obs <= T_max , 
                     sd, NA),
         error = ifelse(obs >= T_min & obs <= T_max , 
                     error, NA))  %>%
  # manual cleaning based on observations
  mutate(obs = ifelse(siteID == "PRLA" & time <ymd("2019-01-01"),
                      NA, obs),
         sd = ifelse(siteID == "PRLA" & time <ymd("2019-01-01"),
                      NA, sd),
         error = ifelse(siteID == "PRLA" & time <ymd("2019-01-01"),
                      NA, error))
 

#==============================================
# targets <- full_join(wq_cleaned, temp_cleaned, by = c("time","siteID")) %>% 
#   select(time, siteID, oxygen, temperature, chla, oxygen_sd, temperature_sd, chla_sd, depth_WQ, depth_temperature)
# 
# targets %>% 
#   select(time, siteID, oxygen, temperature, chla) %>% 
#   pivot_longer(-c("time","siteID"), names_to = "variable", values_to = "value") %>% 
#   ggplot(aes(x = time, y = value)) +
#   geom_point() +
#   facet_grid(variable~siteID, scales = "free")

targets_long <- rbind(wq_cleaned_flagged, temp_cleaned_flagged) %>%
  arrange(siteID, time, variable)

### Write out the targets
write_csv(targets_long, "aquatics-targets (rivers + streams).csv.gz")

## Publish the targets to EFI.  Assumes aws.s3 env vars are configured.
source("../neon4cast-shared-utilities/publish.R")
publish(code = "02_generate_targets_aquatics.R",
        data_out = "aquatics-targets.csv.gz",
        prefix = "aquatics/",
        bucket = "targets",
        provdb = "prov.tsv",
        registries = "https://hash-archive.carlboettiger.info")

message(paste0("Completed Aquatics Target at ", Sys.time()))
