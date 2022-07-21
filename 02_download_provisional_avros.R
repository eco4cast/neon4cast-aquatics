# where should these files be saved?
download_location <- 'C:/Users/freya/Downloads/'
month_use <- '2022-06'

# download the provisional files from Google Cloud Storage using the gsutils tool from cmd
system(paste0('gsutil -m cp -n gs://neon-is-transition-output/provisional/dpid=DP1.20288.001/ms=',
              # -n excludes already downloaded files
              # -m cp runs the copy function in parallel to speed up download
              month_use,
              '/site=PRLA/** ',download_location), ignore.stderr = T)