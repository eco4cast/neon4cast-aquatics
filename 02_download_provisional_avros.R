# where should these files be saved?
download_location <- 'C:/Users/freya/Downloads/'

# need to figure out which month's data are required
# what is in the NEON store db?
cur_wq_month <- format(as.Date(max(wq_cleaned$time)), "%Y-%m")
# what is the next month from this plus the current month? These might be the same
new_month_wq <- unique(format(c((as.Date(max(wq_cleaned$time)) %m+% months(1)), Sys.Date()), "%Y-%m"))


#start by deleting superseded files
# files that have been supersed by the NEON store files can be deleted from the relevent repository
# look in each repository to see if there are files that match the current maximum month of the NEON
# store data
for (i in 1:length(current_sites)) {
  superseded <-
  dir(path = paste0(download_location, current_sites[i]),
      pattern = cur_neon_store_month)
if (length(superseded) != 0) {
  file.remove(paste0(
    paste0(download_location, current_sites[i]),
    '/',
    superseded
  ))
}


##### water quality data ####
for (i in 1:length(current_sites)) {
  # create a directory for each site (if one doesn't already exist)
  if (paste0('site=', current_sites[i]) %in% list.dirs(download_location, full.names = F)) {
    # message('directory exists')
  } else {
    dir.create(path = paste0(download_location, 'site=',current_sites[i]))
    message('creating new directory')
  }
  
  # loop through each month (max 2)
  for (month_use in new_month_wq) {
  # download the provisional files from Google Cloud Storage using the gsutils tool from cmd
  system(paste0('gsutil -m cp -n gs://neon-is-transition-output/provisional/dpid=DP1.20288.001/ms=',
      # -n excludes already downloaded files
      # -m cp runs the copy function in parallel to speed up download
      month_use,
      '/site=',
      current_sites[i],
      "/* ",
      download_location,
      'site=',
      current_sites[i]),
    ignore.stderr = T)
  print(month_use)
  }
 print(paste(current_sites[i], i, '/', length(current_sites)))
}
#===============================#

