# sparklyr package?
library(sparklyr)
library(sparkavro)
library(tidyverse)

# where are the downloaded files
file_path_sc <- '/Users/freya/Downloads/WALK/*.avro'

# times are in UTC
Sys.setenv(TZ = 'UTC')
# spark_install(version = '2.4')
sc <- sparklyr::spark_connect(master = "local")


variables_keep <- c('siteName', 'startDate', 'sensorDepth',
                    'dissolvedOxygen','dissolvedOxygenExpUncert','dissolvedOxygenFinalQF', 
                    'chlorophyll', 'chlorophyllExpUncert','chlorophyllFinalQF')

columns_keep <- c('siteName', 'termName', 'startDate', 'Value', 'sensorDepth')

# read in avro and filter based on variables
wq_avro <- spark_read_avro(sc, name = 'name', path = file_path_sc) %>%
  as_tibble() %>%
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