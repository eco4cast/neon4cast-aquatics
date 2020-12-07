#'# Ecological Forecasting Initiative Null Model 

#'## Set-up

print(paste0("Running Creating Daily Terrestrial Forecasts at ", Sys.time()))

#'Load renv.lock file that includes the versions of all the packages used
#'You can generate using the command renv::snapshot()

#' Required packages.  
#' EFIstandards is at remotes::install_github("eco4cast/EFIstandards")
library(tidyverse)
library(lubridate)
library(rjags)
library(tidybayes)
library(modelr)
library(aws.s3)
library(prov)
library(EFIstandards)
library(EML)
library(jsonlite)

#' set the random number for reproducible MCMC runs
set.seed(329)

#'Generate plot to visualized forecast
generate_plots <- TRUE
#'Is the forecast run on the Ecological Forecasting Initiative Server?
#'Setting to TRUE published the forecast on the server.
efi_server <- TRUE

#' List of team members. Used in the generation of the metadata
#team_list <- list(list(individualName = list(givenName = "Quinn", surName = "Thomas"), 
#                       id = "https://orcid.org/0000-0003-1282-7825"),
#                  list(individualName = list(givenName = "Others",  surName ="Pending")),
#)

#'Team name code
team_name <- "EFInull"

#'Download target file from the server
download.file("https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz",
              "aquatics-targets.csv.gz")

#'Read in target file.  The guess_max is specified because there could be a lot of
#'NA values at the beginning of the file
targets <- read_csv("aquatics-targets.csv.gz", guess_max = 10000)

#'Focal sites
site_names <- c("BARC","POSE")

#'Forecast horizon 
f_days = 7 

#'Generic random walk state-space model is JAGS format.  We use this model for 
#'both the oxygen and temperature null forecasts
RandomWalk = "
model{

  # Priors
  x[1] ~ dnorm(x_ic,tau_init)
  tau_add ~ dgamma(0.1,0.1)
  tau_init ~ dgamma(0.1,0.1)
  
  # Process Model
  for(t in 2:n){
    x[t]~dnorm(x[t-1],tau_add)
    x_obs[t] ~ dnorm(x[t],tau_obs[t])
  }

  # Data Model
  for(i in 1:nobs){
    y[i] ~ dnorm(x[y_wgaps_index[i]], tau_obs[y_wgaps_index[i]])
  }

}
"

#'## NEE Model

#'Create variable for combined forecasts across sites
forecast_saved_oxygen <- NULL
oxygen_figures <- list()
#+ message = FALSE
#' Loop through sites
for(s in 1:length(site_names)){
  
  # Select site
  site_data_var <- targets %>%
    filter(siteID == site_names[s])
  
  # Find the last day in the observed data and add one day for the start of the 
  # forecast
  start_forecast <- max(site_data_var$time) + days(1)
  
  # This is key here - I added the forecast horizon on the end of the data for the forecast period
  full_time <- tibble(time = seq(min(site_data_var$time), max(site_data_var$time) + days(f_days), by = "1 day"))
  
  # Join the full time with the site_data_var so there aren't gaps in the time column
  site_data_var <- left_join(full_time, site_data_var)
  
  #observed oxygen: Full time series with gaps
  y_wgaps <- site_data_var$oxygen
  sd_wgaps <- imputeTS::na_interpolation(site_data_var$oxygen_sd,option = "linear")
  time <- c(site_data_var$time)
  #observed oxygen: time series without gaps
  y_nogaps <- y_wgaps[!is.na(y_wgaps)]
  #Index: time series with gaps
  y_wgaps_index <- 1:length(y_wgaps)
  #Index: the index of the non-NA values in time series with gaps
  y_wgaps_index <- y_wgaps_index[!is.na(y_wgaps)]
  
  #Generate starting initial conditions for latent states
  init_x <- approx(x = time[!is.na(y_wgaps)], y = y_nogaps, xout = time, rule = 2)$y
  
  #Create a list of the data for use in JAGS.  Include vector lengths (nobs, n)
  data <- list(y = y_nogaps,
               y_wgaps_index = y_wgaps_index,
               nobs = length(y_wgaps_index),
               tau_obs = 1/(sd_wgaps ^ 2),
               n = length(y_wgaps),
               x_ic = 0.0)
  
  #Initialize parameters 
  nchain = 3
  chain_seeds <- c(200,800,1400)
  init <- list()
  for(i in 1:nchain){
    init[[i]] <- list(tau_add = 1/var(diff(y_nogaps)),
                      tau_init = mean( 1/var(diff(y_nogaps)), na.rm = TRUE),
                      .RNG.name = "base::Wichmann-Hill",
                      .RNG.seed = chain_seeds[i],
                      x = init_x)
  }
  
  #Initialize JAGS model
  j.model   <- jags.model (file = textConnection(RandomWalk),
                           data = data,
                           inits = init,
                           n.chains = 3)
  
  #Run JAGS model as the burn-in
  jags.out   <- coda.samples(model = j.model,variable.names = c("tau_add","tau_init"), n.iter = 10000)
  
  #Run JAGS model again and sample from the posteriors
  m   <- coda.samples(model = j.model,
                      variable.names = c("x","tau_add","tau_init", "x_obs"),
                      n.iter = 10000,
                      thin = 5)
  
  #Use TidyBayes package to clean up the JAGS output
  model_output <- m %>%
    spread_draws(x_obs[day]) %>%
    filter(.chain == 1) %>%
    rename(ensemble = .iteration) %>%
    mutate(time = full_time$time[day]) %>%
    ungroup() %>%
    select(time, x_obs, ensemble)
  
  if(generate_plots){
    #Pull in the observed data for plotting
    obs <- tibble(time = full_time$time,
                  obs = y_wgaps)
    
    
    #Post past and future
    model_output %>% 
      group_by(time) %>% 
      summarise(mean = mean(x_obs),
                upper = quantile(x_obs, 0.975),
                lower = quantile(x_obs, 0.025),.groups = "drop") %>% 
      ggplot(aes(x = time, y = mean)) +
      geom_line() +
      geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.2, color = "lightblue", fill = "lightblue") +
      geom_point(data = obs, aes(x = time, y = obs), color = "red") +
      labs(x = "Date", y = "oxygen")
    
    ggsave(paste0("oxygen_",site_names[s],"_figure.pdf"), device = "pdf")
  }
  
  #Filter only the forecasted dates and add columns for required variable
  forecast_saved_tmp <- model_output %>%
    filter(time > start_forecast) %>%
    rename(oxygen = x_obs) %>% 
    mutate(data_assimilation = 0,
           forecast = 1,
           obs_flag = 2,
           siteID = site_names[s]) %>%
    mutate(forecast_iteration_id = start_forecast) %>%
    mutate(forecast_project_id = team_name)
  
  # Combined with the previous sites
  forecast_saved_oxygen <- rbind(forecast_saved_oxygen, forecast_saved_tmp)
  
}

#'## temperature model
#' 
#'
#+ message = FALSE


forecast_saved_temperature <- NULL
temperature_figures <- list()
for(s in 1:length(site_names)){
  
  site_data_var <- targets %>%
    filter(siteID == site_names[s])
  
  max_time <- max(site_data_var$time) + days(1)
  
  start_forecast <- max_time
  full_time <- tibble(time = seq(min(site_data_var$time), max(site_data_var$time) + days(f_days), by = "1 day"))
  
  site_data_var <- left_join(full_time, site_data_var)
  
  y_wgaps <- site_data_var$temperature
  sd_wgaps <- imputeTS::na_interpolation(site_data_var$temperature_sd,option = "linear")
  time <- c(site_data_var$time)
  y_nogaps <- y_wgaps[!is.na(y_wgaps)]
  y_wgaps_index <- 1:length(y_wgaps)
  y_wgaps_index <- y_wgaps_index[!is.na(y_wgaps)]
  
  init_x <- approx(x = time[!is.na(y_wgaps)], y = y_nogaps, xout = time, rule = 2)$y
  
  data <- list(y = y_nogaps,
               y_wgaps_index = y_wgaps_index,
               nobs = length(y_wgaps_index),
               tau_obs = 1/(sd_wgaps ^ 2),
               n = length(y_wgaps),
               x_ic = 0.0)
  
  nchain = 3
  chain_seeds <- c(200,800,1400)
  init <- list()
  for(i in 1:nchain){
    init[[i]] <- list(tau_add = 1/var(diff(y_nogaps)),
                      tau_init = mean(1/var(diff(y_nogaps)), na.rm = TRUE),
                      .RNG.name = "base::Wichmann-Hill",
                      .RNG.seed = chain_seeds[i],
                      x = init_x)
  }
  
  j.model   <- jags.model (file = textConnection(RandomWalk),
                           data = data,
                           inits = init,
                           n.chains = 3)
  
  jags.out   <- coda.samples(model = j.model,variable.names = c("tau_add","tau_init"), n.iter = 10000)
  
  m   <- coda.samples(model = j.model,
                      variable.names = c("x","tau_add","tau_init", "x_obs"),
                      n.iter = 10000,
                      thin = 5)
  
  model_output <- m %>%
    spread_draws(x_obs[day]) %>%
    filter(.chain == 1) %>%
    rename(ensemble = .iteration) %>%
    mutate(time = full_time$time[day]) %>%
    ungroup() %>%
    select(time, x_obs, ensemble)
  
  if(generate_plots){
    obs <- tibble(time = full_time$time,
                  obs = y_wgaps)
    
    model_output %>% 
      group_by(time) %>% 
      summarise(mean = mean(x_obs),
                upper = quantile(x_obs, 0.975),
                lower = quantile(x_obs, 0.025),.groups = "drop") %>% 
      ggplot(aes(x = time, y = mean)) +
      geom_line() +
      geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.2, color = "lightblue", fill = "lightblue") +
      geom_point(data = obs, aes(x = time, y = obs), color = "red") +
      labs(x = "Date", y = "temperature")
    
    ggsave(paste0("temperature_daily_",site_names[s],"_figure.pdf"), device = "pdf")
  }
  
  forecast_saved_tmp <- model_output %>%
    filter(time > start_forecast) %>%
    rename(temperature = x_obs) %>% 
    mutate(data_assimilation = 0,
           forecast = 1,
           obs_flag = 2,
           siteID = site_names[s]) %>%
    mutate(forecast_iteration_id = start_forecast) %>%
    mutate(forecast_project_id = team_name)
  
  forecast_saved_temperature <- rbind(forecast_saved_temperature, forecast_saved_tmp)
}

#'Combined the oxygen and temperature forecasts together and re-order column
forecast_saved <- cbind(forecast_saved_oxygen, forecast_saved_temperature$temperature) %>% 
  rename(temperature = `forecast_saved_temperature$temperature`) %>% 
  select(time, ensemble, siteID, oxygen, temperature, obs_flag, forecast, data_assimilation)

#'Save file as CSV in the
#'[theme_name]-[year]-[month]-[date]-[team_name].csv
forecast_file_name_base <- paste0("aquatics-",as_date(start_forecast),"-",team_name)
forecast_file <- paste0(forecast_file_name_base, ".csv.gz")
write_csv(forecast_saved, forecast_file)

#'#Generate metadata

#'Get system time for setting the issue time of the forecast
curr_time <- with_tz(Sys.time(), "UTC")
#forecast_issue_time <- format(curr_time,format = "%Y-%m-%d %H:%M:%SZ", usetz = F)
forecast_issue_time <- as_date(curr_time)
forecast_iteration_id <- start_forecast

#' The team name is the `forecast_model_id`
forecast_model_id <- team_name

source("metadata/generate_metadata.R")

meta_data_filename <- generate_metadata(forecast_file =  forecast_file,
                                        metadata_yaml = "metadata/metadata.yml",
                                        forecast_issue_time = as_date(with_tz(Sys.time(), "UTC")),
                                        forecast_iteration_id = start_forecast,
                                        forecast_file_name_base = forecast_file_name_base)


#'Publish the forecast automatically.  Run only on EFI Challenge server
if(efi_server){
  source("../neon4cast-shared-utilities/publish.R")
  publish(code = "03_generate_null_forecast_aquatics.R",
          data_in = "aquatics-targets.csv.gz",
          data_out = forecast_file,
          meta = meta_data_filename,
          prefix = "aquatics/",
          bucket = "forecasts")
}
