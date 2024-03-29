library(cronR)

home_dir <- "/home/rstudio"
log_dir <- "/home/rstudio/log/cron"

aquatics_repo <- "neon4cast-aquatics"

#Go to healthchecks.io. Create a project.  Add a check. Copy the url and add here.  
health_checks_url <- "https://hc-ping.com/a848914e-9abf-45e4-bcf3-27f570cc3623"

cmd <- cronR::cron_rscript(rscript = file.path(home_dir, aquatics_repo,"03_climatology_null.R"),
                           rscript_log = file.path(log_dir, "aquatics-climatology.log"),
                           log_append = FALSE,
                           workdir = file.path(home_dir, aquatics_repo),
                           trailing_arg = paste0("curl -fsS -m 10 --retry 5 -o /dev/null ", health_checks_url))
cronR::cron_add(command = cmd, frequency = 'daily', at = "11AM", id = 'aquatics-null-models')