renv::restore()

library(tidyverse)
library(lubridate)
library(rjags)
library(tidybayes)
library(modelr)

set.seed(329)

download.file("https://data.ecoforecast.org/targets/aquatics/aquatic-oxygen-targets.csv.gz",
              "aquatic-oxygen-targets.csv.gz")

aquatic_targets <- read_csv("aquatic-oxygen-targets.csv.gz", guess_max = 10000)
oxygen <- aquatic_targets %>%
  filter(siteID == "BARC",
         time > as_date("2020-01-01"),
         hour(time) == 12) %>%
  select(time, siteID, dissolved_oxygen, dissolved_oxygen_sd)

oxygen_sd <- mean(aquatic_targets$dissolved_oxygen_sd, na.rm = TRUE)


#max_time <- max(oxygen$time) - months(1) +  days(1)
max_time <- max(oxygen$time) + days(1)

#This is for testing
oxygen <- oxygen %>%
  filter(time < max_time)

start_forecast <- max_time
# This is key here - I added 16 days on the end of the data for the forecast period
full_time <- tibble(time = seq(min(oxygen$time), max(oxygen$time) + days(16), by = "1 day"))

oxygen <- left_join(full_time, oxygen)

RandomWalk = "
model{

  #### Priors
  x[1] ~ dnorm(x_ic,tau_ic)
  tau_add <- 1 / pow(sd_add,2)
  sd_add ~ dunif(lower_add,upper_add)

  #### Process Model
  for(t in 2:n){
    x[t]~dnorm(x[t-1],tau_add)
  }

  #### Data Model
  for(t in 1:nobs){
    y[t] ~ dnorm(x[y_index[t]],tau_obs)
  }

}
"

#Full time series with gaps
y <- c(oxygen$dissolved_oxygen)
time <- c(oxygen$time)
#Indexes of full time series with gaps
y_index <- 1:length(y)
#Remove gaps
y_gaps <- y[!is.na(y)]
#keep indexes to reference the gappy time series
y_index <- y_index[!is.na(y)]

init_x <- approx(x = time[!is.na(y)], y = y_gaps, xout = time, rule = 2)$y

data <- list(y = y_gaps,
             y_index = y_index,
             nobs = length(y_index),
             n = length(y),
             x_ic = 8.620833,
             tau_ic = 51,
             tau_obs = 1/(oxygen_sd^2),
             lower_add=0.0001,
             upper_add=1000)

nchain = 3
chain_seeds <- c(200,800,1400)
init <- list()
for(i in 1:nchain){
  init[[i]] <- list(sd_add=sd(diff(y_gaps)),
                    .RNG.name = "base::Wichmann-Hill",
                    .RNG.seed = chain_seeds[i],
                    x = init_x)
}

j.model   <- jags.model (file = textConnection(RandomWalk),
                         data = data,
                         inits = init,
                         n.chains = 3)

jags.out   <- coda.samples (model = j.model,variable.names = "sd_add", n.iter = 10000)

m   <- coda.samples (model = j.model,
                     variable.names = c("x","sd_add"),
                     n.iter = 10000,
                     thin = 5)

model_output <- m %>%
  spread_draws(x[day]) %>%
  filter(.chain == 1) %>%
  rename(oxygen = x,
         ensemble = .iteration) %>%
  mutate(time = full_time$time[day]) %>%
  ungroup() %>%
  select(time, oxygen, ensemble) %>%
  mutate(oxygen_w_obs_error = rnorm(n(), mean = oxygen, sd = oxygen_sd))

forecast_saved <- model_output %>%
  filter(time > start_forecast) %>%
  mutate(data_assimilation = 0) %>%
  mutate(forecast_iteration_id = start_forecast) %>%
  mutate(forecast_project_id = "EFInull")

forecast_file_name <- paste0("aquatics-oxygen-EFInull-",as_date(start_forecast),".csv.gz")
write_csv(forecast_saved, forecast_file_name)

## Publish the forecast automatically. (EFI-only)

source("../NEON-community-forecast/R/publish.R")
publish(code = "03_generate_null_forecast_aquatics.R",
        data_in = "aquatic-oxygen-targets.csv.gz",
        data_out = forecast_file_name,
        prefix = "aquatics/",
        bucket = "forecasts")
