
download.neon.avro <- function(months, sites, data_product, path) {
  for (i in 1:length(sites)) {
    # create a directory for each site (if one doesn't already exist)
    if (paste0('site=', sites[i]) %in% list.dirs(path, full.names = F)) {
      # message('directory exists')
    } else {
      dir.create(path = paste0(path, '/site=', sites[i]), recursive = TRUE, showWarnings = FALSE)
      message('creating new directory')
    }
    
    message(paste0('downloading site ', i, '/', length(sites)))
    
    # loop through each month (max 2) and each dp listed
    for (month_use in months) {
      for (dp in data_product) {
        # download the provisional files from Google Cloud Storage using the gsutils tool from cmd
        # -n argument skips files already downloaded
        system(paste0('~/google-cloud-sdk/bin/gsutil -m cp -n gs://neon-is-transition-output/provisional/dpid=DP1.',
                      dp,
                      '.001/ms=',
                      # -n excludes already downloaded files
                      # -m cp runs the copy function in parallel to speed up download
                      month_use,
                      '/site=',
                      sites[i],
                      "/* ",
                      path,
                      '/site=',
                      sites[i]),
               ignore.stderr = T)
        
      }
      
    }
   
  }
}
# no error is thrown if the data product doesn't exist for that site
 

# delete the superseded files
delete.neon.avro <- function(months, sites, path, data_product) {
  for (i in 1:length(sites)) {
    superseded <-  dir(path = paste0(path, '/site=', sites[i]),
                       pattern = months) %>% .[matches(data_product, vars=.)]
    
    if (length(superseded) != 0) {
      file.remove(paste0(path,'/site=', sites[i], '/',superseded))
    }
  }
}

# requires wq_vars - specify
read.avro.wq <- function(sc, name = 'name', path) {
  message(paste0('reading file ', path))
  wq_avro <- sparkavro::spark_read_avro(sc, 
                                        name = "name",
                                        path = path,
                                        memory = FALSE) |> 
    dplyr::filter(termName %in% wq_vars) %>%
    # for streams want to omit the downstream measurement (102) and retain upstream (101)
    # rivers and lakes horizontal index is 103
  dplyr::filter(horizontalIndex %in% c('101', '111', '103')) %>%
  dplyr::select(siteName, termName, startDate, 
           doubleValue, intValue) %>%
    dplyr::collect() |> 
    # combine the value fields to one
    dplyr::mutate(Value = ifelse(is.na(doubleValue), 
                          intValue, doubleValue)) %>%
    dplyr::select(any_of(columns_keep))

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
               chlorophyll = ifelse('chlorophyll' %in% colnames(wq_tibble_wider), chlorophyll, NA),
               chlorophyllExpUncert = ifelse('chlorophyll' %in% colnames(wq_tibble_wider),chlorophyllExpUncert,NA)) %>%
        group_by(siteName, time) %>%
        summarize(oxygen = mean(dissolvedOxygen, na.rm = TRUE),
                  chla = mean(chlorophyll, na.rm = TRUE),.groups = "drop") %>%
        rename(site_id = siteName) %>%
        # get in the same long format as the NEON portal data
        pivot_longer(cols = -c("time", "site_id"), 
                     names_to = "variable", 
                     values_to = "observed")
    } 
  }
  
  if (exists('daily_wq')) {
    if (unique(daily_wq$site_id) %in% stream_sites) {
      daily_wq <- daily_wq %>% dplyr::filter(variable == "oxygen")
    }
  }
  
  if (exists('daily_wq')) {
    return(daily_wq)
  } else {
      # create an empty df to return
      empty <- data.frame(site_id = NA, 
                          time = NA, 
                          variable = NA,
                          observed = NA)  %>%
        mutate(site_id = as.character(site_id),
               time = as.Date(time),
               variable = as.character(variable),
               observed = as.numeric(observed)) %>%
        dplyr::filter(rowSums(is.na(.)) != ncol(.)) # remove the empty row
      return(empty)
  }
}

read.avro.tsd <- function(sc, name = 'name', path, thermistor_depths) {
  message(paste0('reading file ', path))
  tsd_avro <- sparkavro::spark_read_avro(sc, 
                                         name = "name",
                                         path = path,
                                         memory = FALSE) |> 
    dplyr::filter(termName %in% tsd_vars) %>%
    # for streams want to omit the downstream measurement (102) and retain upstream (101)
    # rivers and lakes horizontal index is 103
    dplyr::filter(horizontalIndex %in% c('101','111', '103'), 
           temporalIndex == "030") %>% # take the 30-minutely data only
    dplyr::select(siteName, termName, startDate, 
           doubleValue, intValue, verticalIndex) %>%
    collect() |> 
    # combine the value fields to one
    dplyr::mutate(Value = ifelse(is.na(doubleValue), 
                          intValue, doubleValue)) %>%
    dplyr::select(any_of(columns_keep)) 
  
  
  tsd_tibble <- tsd_avro %>%
    as.data.frame() %>%
    suppressWarnings() %>%
    inner_join(., thermistor_depths, by = c('siteName', 'verticalIndex')) %>%
    # only want temperatures in the surface (< 1 m) of the water column
    dplyr::filter(thermistorDepth <= 1.0) 
  
  if (nrow(tsd_tibble) >=1) {
    tsd_tibble_wider <- tsd_tibble %>%
      arrange(startDate, termName) %>%
      pivot_wider(names_from = termName, values_from = Value) %>%
      filter_at(vars(ends_with('QF')), any_vars(. != 1)) # checks to see if any of the QF cols have a 1
    
    # if the filtering has left rows then find the mean
    if (nrow(tsd_tibble_wider) >= 1) {
      daily_tsd <- tsd_tibble_wider  %>%
        mutate(time = as.Date(startDate)) %>%
        # add missing columns (sites with no tsdWaterTempMean), need all otherwise function won't run
        # tsdWaterTempMean = ifelse('tsdWaterTempMean' %in% colnames(tsd_tibble_wider), 
        #                           tsdWaterTempMean, NA),
        # tsdWaterTempExpUncert = ifelse('tsdWaterTempExpUncert' %in% colnames(tsd_tibble_wider),
        #                                tsdWaterTempExpUncert, NA)) %>%
        group_by(siteName, time) %>%
        summarize(temperature = mean(tsdWaterTempMean, na.rm = TRUE),
                  .groups = "drop") %>%
        rename(site_id = siteName) %>%
        # get in the same long format as the NEON portal data
        pivot_longer(cols = -c("time", "site_id"), 
                     names_to = "variable", 
                     values_to = "observed")
      
    } 
  }
  
  if (exists('daily_tsd')) {
    return(daily_tsd)
  } else {
    # create an empty df to return
    empty <- data.frame(site_id = NA,
                        time = NA, 
                        variable = NA, 
                        observed = NA)  %>%
      mutate(site_id = as.character(site_id),
             time = as.Date(time),
             variable = as.character(variable),
             observed = as.numeric(observed)) %>%
      dplyr::filter(rowSums(is.na(.)) != ncol(.)) # remove the empty row
    return(empty)
  }
}


read.avro.tsd.profile <- function(sc, name = 'name', path, thermistor_depths) {
  message(paste0('reading file ', path))

    tsd_avro <- sparkavro::spark_read_avro(sc, 
                                           name = "name",
                                           path = path,
                                           memory = FALSE) %>%
    dplyr::filter(termName %in% tsd_vars) %>%
    # for streams want to omit the downstream measurement (102) and retain upstream (101)
    # rivers and lakes horizontal index is 103
    dplyr::filter(horizontalIndex %in% c('101', '111', '103'), 
           temporalIndex == "030") %>% # take the 30-minutely data only
    dplyr::select(siteName, termName, startDate, 
           doubleValue, intValue, verticalIndex) %>%
    collect() |> 
    # combine the value fields to one
    dplyr::mutate(Value = ifelse(is.na(doubleValue), 
                          intValue, doubleValue)) %>%
    dplyr::select(any_of(columns_keep)) 
  
  
  tsd_tibble <- tsd_avro %>%
    as.data.frame() %>%
    suppressWarnings() %>%
    inner_join(., thermistor_depths, by = c('siteName', 'verticalIndex')) 
  
  if (nrow(tsd_tibble) >=1) {
    tsd_tibble_wider <- tsd_tibble %>%
      arrange(startDate, termName) %>%
      pivot_wider(names_from = termName, values_from = Value) %>%
      filter_at(vars(ends_with('QF')), any_vars(. != 1)) # checks to see if any of the QF cols have a 1
    
    # if the filtering has left rows then find the mean
    if (nrow(tsd_tibble_wider) >= 1) {
      hourly_tsd <- tsd_tibble_wider  %>%
        mutate(time = lubridate::ymd_h(format(startDate, "%Y-%m-%d %H"))) %>%
        # add missing columns (sites with no tsdWaterTempMean), need all otherwise function won't run
        # tsdWaterTempMean = ifelse('tsdWaterTempMean' %in% colnames(tsd_tibble_wider), 
        #                           tsdWaterTempMean, NA),
        # tsdWaterTempExpUncert = ifelse('tsdWaterTempExpUncert' %in% colnames(tsd_tibble_wider),
        #                                tsdWaterTempExpUncert, NA)) %>%
        group_by(siteName, time, thermistorDepth) %>%
        summarize(temperature = mean(tsdWaterTempMean, na.rm = TRUE),
                  .groups = "drop") %>%
        rename(site_id = siteName,
               depth = thermistorDepth) %>%
        # get in the same long format as the NEON portal data
        pivot_longer(cols = -c("time", "site_id", "depth"), 
                     names_to = "variable", 
                     values_to = "observed")
      
    } 
  }
  
  if (exists('hourly_tsd')) {
    return(hourly_tsd)
  } else {
    # create an empty df to return
    empty <- data.frame(site_id = NA,
                        time = NA, 
                        depth = NA,
                        variable = NA, 
                        observed = NA)  %>%
      mutate(site_id = as.character(site_id),
             time = as.Date(time),
             depth = as.numeric(depth),
             variable = as.character(variable),
             observed = as.numeric(observed)) %>%
      dplyr::filter(rowSums(is.na(.)) != ncol(.)) # remove the empty row
    return(empty)
  }
}


read.avro.prt <- function(sc, name = 'name', path) {
  message(paste0('reading file ', path))
  prt_avro <- sparkavro::spark_read_avro(sc, name = 'name', 
                                         path = path) |> 
    dplyr::filter(termName %in% prt_vars) %>%
    # for streams want to omit the downstream measurement (102) and retain upstream (101)
    # rivers and lakes horizontal index is 103
      dplyr::filter(horizontalIndex %in% c('101', '111', '103'), 
           temporalIndex == "030") %>% # take the 30-minutely data only
      dplyr::select(siteName, termName, startDate, 
           doubleValue, intValue) %>%
    collect() |> 
    # combine the value fields to one
      dplyr::mutate(Value = ifelse(is.na(doubleValue), 
                          intValue, doubleValue)) %>%
      dplyr::select(any_of(columns_keep)) 
  
  
  prt_tibble <- prt_avro %>%
    as.data.frame() %>%
    suppressWarnings()
  
  if (nrow(prt_tibble) >=1) {
    prt_tibble_wider <- prt_tibble %>%
      arrange(startDate, termName) %>%
      pivot_wider(names_from = termName, values_from = Value) %>%
      filter_at(vars(ends_with('QF')), any_vars(. != 1)) # checks to see if any of the QF cols have a 1
    
    # if the filtering has left rows then find the mean
    if (nrow(prt_tibble_wider) >= 1) {
      daily_prt <- prt_tibble_wider  %>%
        mutate(time = as.Date(startDate)) %>%
        group_by(siteName, time) %>%
        summarize(temperature = mean(surfWaterTempMean, na.rm = TRUE),
                  .groups = "drop") %>%
        rename(site_id = siteName) %>%
        rename(observation = temperature) |> 
        mutate(variable = "temperature")
      
    } 
  }
  
  if (exists('daily_prt')) {
    return(daily_prt)
  } else {
    # create an empty df to return
    empty <- data.frame(site_id = NA, 
                        time = NA, 
                        variable = NA, 
                        observed = NA)  %>%
      mutate(site_id = as.character(site_id),
             time = as.Date(time),
             variable = as.character(variable),
             observed = as.numeric(observed)) %>%
      dplyr::filter(rowSums(is.na(.)) != ncol(.)) # remove the empty row
    return(empty)
  }
}


# wrapper function for a generic read avro function that selects the right 
  # read_avro_... depending on the data product
read.neon.avro <- function(path, sc, data_product, ...) {
  # the function is only available for 3 data products (wq, tsd, and prt)
  if (data_product != '20288' |
      data_product != '20264' |
      data_product != '20053') {
    stop('No read avro function for this data product')
  }
  
  #does the data product match the file path specified 
  if (str_detect(path, data_product) == FALSE) {
    stop('Data product does not match path')
  }
  
  # use the right function based on the data product specified
  if (data_product == '20288') {
    message('running read.avro for water quality')
    df <- read.avro.wq(path = path, sc = sc)
  } else if (data_product == '20264') {
    message('running read.avro for temperature at specific depth')
    df <- read.avro.tsd(path = path, sc = sc, thermistor_depths = thermistor_depths)
  } else if (data_product == '20053') {
    message('running read.avro for surface temperature')
    df <- read.avro.prt(path = path, sc = sc)
  }
  return(df)
} 