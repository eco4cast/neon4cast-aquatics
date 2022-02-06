library(cronR)

home_dir <- "/home/rstudio/Documents/scripts"
log_dir <- "/efi_neon_challenge/log/cron"

terrestrial_repo <- "neon4cast-terrestrial"

#Go to healthchecks.io. Create a project.  Add a check. Copy the url and add here.  
health_checks_url <- "https://hc-ping.com/bbe3ddc7-4020-4c53-bb13-08580d765e32"

cmd <- cronR::cron_rscript(rscript = file.path(home_dir, terrestrial_repo,"run-terrestrial-null-models.R"),
                           rscript_log = file.path(log_dir, "terrestrial.log"),
                           log_append = FALSE,
                           workdir = file.path(home_dir, terrestrial_repo),
                           trailing_arg = paste0("curl -fsS -m 10 --retry 5 -o /dev/null ", ))
cronR::cron_add(command = cmd, frequency = 'daily', at = "11AM", id = 'terrestrial-null-models')