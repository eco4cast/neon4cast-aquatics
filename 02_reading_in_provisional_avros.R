# sparklyr package?
library(sparklyr)
library(sparkavro)
library(tidyverse)

# where are the downloaded files
file_path_sc <- '/Users/freya/Downloads/'

# times are in UTC
Sys.setenv(TZ = 'UTC')
# spark_install(version = '2.4')
sc <- sparklyr::spark_connect(master = "local")


variables_keep <- c('siteName', 'startDate', 'sensorDepth',
                    'dissolvedOxygen','dissolvedOxygenExpUncert','dissolvedOxygenFinalQF', 
                    'chlorophyll', 'chlorophyllExpUncert','chlorophyllFinalQF')

columns_keep <- c('siteName', 'termName', 'startDate', 'Value', 'sensorDepth')

provisional_data <- data.frame('siteID' = NA, 'time' = NA, 'variable' = NA, 'obs' = NA, 'error' = NA)

for (i in 1:length(current_sites)) {
  # read in avro and filter based on variables
  wq_avro <- spark_read_avro(sc, name = 'name', path = paste0(file_path_sc, current_sites[i], '/*.avro')) %>%
    filter(termName %in% variables_keep) %>%
    # for streams want to omit the downstream measurement (102) and retain upstream (101)
    # rivers and lakes horizontal index is 103
    filter(horizontalIndex %in% c('101', '103')) %>%
    select(siteName, termName, startDate, 
           doubleValue, intValue) %>%
    # combine the value fields to one
    mutate(Value = ifelse(is.na(doubleValue), 
                          intValue, doubleValue)) %>%
    select(any_of(columns_keep))
  
  wq_tibble <- wq_avro %>%
    as_tibble() 
  
  if (nrow(wq_tibble) >=1) {
    wq_tibble <- wq_tibble %>%
      arrange(startDate, termName) %>%
      pivot_wider(names_from = termName, values_from = Value)  %>%
      filter_at(vars(ends_with('QF')), any_vars(. !=1)) # checks to see if any of the QF cols have a 1
    
  }

  # if the filtering has left rows then find the mean
  if (nrow(wq_tibble) >=1) {
    daily_wq <- wq_tibble  %>%
      mutate(time = as.Date(startDate),
             # add missing columns
             chlorophyll = ifelse('chlorophyll' %in% colnames(wq_tibble), chlorophyll, NA),
             chlorophyllExpUncert = ifelse('chlorophyll' %in% colnames(wq_tibble), chlorophyllExpUncert, NA)) %>%
      group_by(siteName, time) %>%
      summarize(oxygen_obs = mean(dissolvedOxygen, na.rm = TRUE),
                chla_obs = mean(chlorophyll, na.rm = TRUE),
                #why only using the count of non-NA in DO?
                count = sum(!is.na(dissolvedOxygen)),
                chla_error = mean(chlorophyllExpUncert, na.rm = TRUE)/sqrt(count),
                oxygen_error = mean(dissolvedOxygenExpUncert, na.rm = TRUE)/sqrt(count),.groups = "drop") %>%
      rename(siteID = siteName) %>%
      select(-count) %>%
      pivot_longer(cols = !c(time, siteID), names_to = c("variable", "stat"), names_sep = "_") %>%
      pivot_wider(names_from = stat, values_from = value)
    
    provisional_data <- bind_rows(daily_wq, provisional_data)
  }
  print(paste0(i, '/', length(current_sites)))
}


