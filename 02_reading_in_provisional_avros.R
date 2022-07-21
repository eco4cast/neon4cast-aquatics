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

wq_avro <- spark_read_avro(sc, name = 'name', path = file_path_sc) %>%
  as_tibble()