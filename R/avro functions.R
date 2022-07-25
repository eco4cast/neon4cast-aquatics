
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
 

# delete the superseded files
delete_neon_avro <- function(months, sites, path) {
  for (i in 1:length(sites)) {
    superseded <-  dir(path = paste0(path, 'site=', sites[i]),
                       pattern = months)
    
    if (length(superseded) != 0) {
      file.remove(paste0(path,'site=', sites[i], '/',superseded))
    }
  }
}

# requires wq_vars - specify
read.avro.wq <- function(sc, name = 'name', path = path) {
  message(paste0('reading file ', path))
  wq_avro <- sparkavro::spark_read_avro(sc, name = 'name', 
                                        path = path) %>%
    filter(termName %in% wq_vars) %>%
    # for streams want to omit the downstream measurement (102) and retain upstream (101)
    # rivers and lakes horizontal index is 103
    filter(horizontalIndex %in% c('101', '103')) %>%
    select(siteName, termName, startDate, 
           doubleValue, intValue) %>%
    # combine the value fields to one
    mutate(Value = ifelse(is.na(doubleValue), 
                          intValue, doubleValue)) %>%
    select(any_of(columns_keep))
  
  wq_tibble <- wq_avro %>%
    as.data.frame() %>%
    suppressWarnings()
  
  if (nrow(wq_tibble) >=1) {
    wq_tibble_wider <- wq_tibble %>%
      arrange(startDate, termName) %>%
      pivot_wider(names_from = termName, values_from = Value)  %>%
      filter_at(vars(ends_with('QF')), any_vars(. != 1)) # checks to see if any of the QF cols have a 1
    
    # if the filtering has left rows then find the mean
    if (nrow(wq_tibble_wider) >= 1) {
      daily_wq <- wq_tibble_wider  %>%
        mutate(time = as.Date(startDate),
               # add missing columns (sites with no chlorophyll), need all otherwise function won't run
               chlorophyll = ifelse('chlorophyll' %in% colnames(wq_tibble), chlorophyll, NA),
               chlorophyllExpUncert = ifelse('chlorophyll' %in% colnames(wq_tibble),chlorophyllExpUncert,NA)) %>%
        group_by(siteName, time) %>%
        summarize(oxygen_obs = mean(dissolvedOxygen, na.rm = TRUE),
                  chla_obs = mean(chlorophyll, na.rm = TRUE),
                  count = sum(!is.na(dissolvedOxygen)),
                  chla_error = mean(chlorophyllExpUncert, na.rm = TRUE) / sqrt(count),
                  oxygen_error = mean(dissolvedOxygenExpUncert, na.rm = TRUE) / sqrt(count),.groups = "drop") %>%
        rename(siteID = siteName) %>%
        select(-count) %>%
        # get in the same long format as the NEON portal data
        pivot_longer(cols = !c(time, siteID), 
                     names_to = c("variable", "stat"), 
                     names_sep = "_") %>%
        pivot_wider(names_from = stat, values_from = value)
      
    } 
  }
  
  if (exists('daily_wq')) {
    return(daily_wq)
  } else {
      # create an empty df to return
      empty <- data.frame(siteID = NA, time = NA, variable = NA, obs = NA, error = NA)  %>%
        mutate(siteID = as.character(siteID),
               time = as.Date(time),
               variable = as.character(variable),
               obs = as.numeric(obs),
               error = as.numeric(error)) %>%
        filter(rowSums(is.na(.)) != ncol(.)) # remove the empty row
      return(empty)
  }
}

