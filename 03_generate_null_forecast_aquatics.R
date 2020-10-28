print(paste0("Generating aquatic null forecast ", Sys.time()))

renv::restore()

library(tidyverse)
library(lubridate)
library(rjags)
library(tidybayes)
library(modelr)

set.seed(329)
generate_plots <- TRUE

download.file("https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz",
              "aquatics-targets.csv.gz")

aquatic_targets <- read_csv("aquatics-targets.csv.gz", guess_max = 10000)
oxygen <- aquatic_targets %>%
  filter(siteID == "BARC", #need to add POSE to the list
         time > as_date("2020-01-01")) %>% 
  select(time, siteID, oxygen, oxygen_sd)

oxygen_sd <- mean(aquatic_targets$oxygen_sd, na.rm = TRUE)


#max_time <- max(oxygen$time) - months(1) +  days(1)
max_time <- max(oxygen$time) + days(1)

#This is for testing
oxygen <- oxygen %>%
  filter(time < max_time)

start_forecast <- max_time
# This is key here - I added 16 days on the end of the data for the forecast period
full_time <- tibble(time = seq(min(oxygen$time), max(oxygen$time) + days(7), by = "1 day"))

oxygen <- left_join(full_time, oxygen)

RandomWalk = "
model{

  # Priors
  x[1] ~ dnorm(x_ic,tau_obs[1])
  tau_add ~ dgamma(0.1,0.1)

  # Process Model
  for(t in 2:n){
    x[t]~dnorm(x[t-1],tau_add)
    x_obs[t] ~ dnorm(x[t],tau_obs_mean)
  }

  # Data Model
  for(i in 1:nobs){
    y[i] ~ dnorm(x[y_wgaps_index[i]], tau_obs[i])
  }

}
"

sd_wgaps <- oxygen$oxygen_sd
y_wgaps <- oxygen$oxygen
time <- oxygen$time

y_nogaps <- y_wgaps[!is.na(y_wgaps)]
sd_nogaps <- sd_wgaps[!is.na(y_wgaps)]

y_wgaps_index <- 1:length(y_wgaps)

y_wgaps_index <- y_wgaps_index[!is.na(y_wgaps)]

init_x <- approx(x = time[!is.na(y_wgaps)], y = y_nogaps, xout = time, rule = 2)$y

data <- list(y = y_nogaps,
             y_wgaps_index = y_wgaps_index,
             nobs = length(y_wgaps_index),
             n = length(y_wgaps),
             x_ic = y_nogaps[1],
             tau_obs_mean = mean(1/(sd_nogaps^2)),
             tau_obs = 1/(sd_nogaps^2))

nchain = 3
chain_seeds <- c(200,800,1400)
init <- list()
for(i in 1:nchain){
  init[[i]] <- list(tau_add = 1/var(diff(y_nogaps)),
                    .RNG.name = "base::Wichmann-Hill",
                    .RNG.seed = chain_seeds[i],
                    x = init_x)
}

j.model   <- jags.model(file = textConnection(RandomWalk),
                         data = data,
                         inits = init,
                         n.chains = 3)

jags.out   <- coda.samples(model = j.model,variable.names = c("tau_add"), n.iter = 10000)

m   <- coda.samples(model = j.model,
                    variable.names = c("x","tau_add", "x_obs"),
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
    labs(x = "Date", y = "oxygen")
}

forecast_saved <- model_output %>%
  filter(time > start_forecast) %>%
  rename(oxygen = x_obs) %>% 
  mutate(data_assimilation = 0) %>%
  mutate(forecast_iteration_id = start_forecast) %>%
  mutate(forecast_project_id = "EFInull")


forecast_file_name <- paste0("aquatics-",as_date(start_forecast),"-EFIpernull.csv.gz")
print(paste0("Writing forecast to ", forecast_file_name))
write_csv(forecast_saved, forecast_file_name)

## Publish the forecast automatically. (EFI-only)

source("../neon4cast-shared-utilities/publish.R")
print("Publishing forecast to bucket")
publish(code = "03_generate_null_forecast_aquatics.R",
        data_in = "aquatics-targets.csv.gz",
        data_out = forecast_file_name,
        prefix = "aquatics/",
        bucket = "forecasts")

print(paste0("Finished aquatic null forecast ", Sys.time()))


