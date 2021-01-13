message(paste0("Running Creating Aquatics Targets at ", Sys.time()))
#remotes::install_github("cboettig/neonstore")

devtools::install_deps()

## 02_generate_targets_aquatics
## Process the raw data into the target variable product
library(neonstore)
library(tidyverse)
library(lubridate)
library(contentid)

run_full_workflow <- TRUE
generate_null <- TRUE


# Aquatic
#DP1.20053.001
#DP1.20288.001
sites <- c("BARC", "POSE")
start_date <- NA

message("Downloading: DP1.20288.001")
new_data1 <- neonstore::neon_download("DP1.20288.001",site = sites, type = "basic", start_date = start_date, .token = Sys.getenv("NEON_TOKEN"))
message("Downloading: DP1.20264.001")
new_data2 <- neonstore::neon_download("DP1.20264.001", site =  sites, type = "basic", start_date = start_date, .token = Sys.getenv("NEON_TOKEN"))
message("Downloading: DP1.20053.001")
new_data3 <- neonstore::neon_download("DP1.20053.001", site =  sites, type = "basic", start_date = start_date, .token = Sys.getenv("NEON_TOKEN"))


if(!is.null(new_data1) | !is.null(new_data2) | !is.null(new_data3) | run_full_workflow){
  
  message(paste0("Running Creating Aquatics Targets at ", Sys.time()))
  
  source("02_generate_targets_aquatics.R")
  
  message(paste0("Completed Aquatics Target at ", Sys.time()))
  
  if(generate_null){
    
    message(paste0("Running Creating Aquatics Null at ", Sys.time()))
    source("03_generate_null_forecast_aquatics.R")
  }
  
}
