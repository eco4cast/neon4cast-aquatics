
download_neon_avro <- function(months, sites, data_product, path) {
  for (i in 1:length(sites)) {
    # create a directory for each site (if one doesn't already exist)
    if (paste0('site=', sites[i]) %in% list.dirs(path, full.names = F)) {
      # message('directory exists')
    } else {
      dir.create(path = paste0(path, 'site=', sites[i]))
      message('creating new directory')
    }
    
    message(paste0('downloading site ', i, '/', length(sites)))
    
    # loop through each month (max 2) and each dp listed
    for (month_use in months) {
      for (dp in data_product) {
        # download the provisional files from Google Cloud Storage using the gsutils tool from cmd
        # -n argument skips files already downloaded
        system(paste0('gsutil -m cp -n gs://neon-is-transition-output/provisional/dpid=DP1.',
                      dp,
                      '.001/ms=',
                      # -n excludes already downloaded files
                      # -m cp runs the copy function in parallel to speed up download
                      month_use,
                      '/site=',
                      sites[i],
                      "/* ",
                      path,
                      'site=',
                      sites[i]),
               ignore.stderr = T)
        
      }
      
    }
   
  }
}
# no error is thrown if the data product doesn't exist for that site
 
