#### Temperature data QC ####
# Function to QC temperature profiles 
  # option to group by site and depth or just by site
  # need to specify the 1) absolute range (vector - length 2) and the 2) the acceptable rate of change (absolute value)
QC.temp <- function(df, range, spike, by.depth = T) {
  if (by.depth == F) {
    df_QC <- df %>%
      arrange(site_id, time) %>%
      group_by(site_id) %>%
      mutate(temp_change = observed - dplyr::lag(observed), 
             
             flagged_abs_val = ifelse(between(observed, min(range), max(range)), F, T),
             flagged_spike = ifelse(abs(temp_change) > spike,
                                    T, F),
             flagged_flat = ifelse(temp_change == 0,
                                   T, F),
             final_flag = ifelse(flagged_spike != F | 
                                   flagged_abs_val != F |
                                   flagged_flat != F,
                                 T, F),
             observed = ifelse(final_flag == T, 
                                  NA, observed)) %>%
      select(-c(contains('flag'), temp_change))
  } else {
    df_QC <- df %>%
      arrange(site_id, time) %>%
      group_by(site_id, depth) %>%
      mutate(temp_change = observed - dplyr::lag(observed), 
             
             flagged_abs_val = ifelse(between(observed, min(range), max(range)), F, T),
             flagged_spike = ifelse(abs(temp_change) > spike,
                                    T, F),
             flagged_flat = ifelse(temp_change == 0,
                                   T, F),
             final_flag = ifelse((flagged_spike == T | 
                                   flagged_abs_val == T |
                                   flagged_flat == T),
                                 T, F),
             observed = ifelse((final_flag == F | is.na(final_flag)), 
                                  observed, NA)) %>%
      select(-c(contains('flag'), temp_change))
  }
  return(df_QC)
}


#### Standard Error ####
# Function to calculate the standard error of the mean
se <- function(x) {
  se <- sd(x, na.rm = T)/(sqrt(length(which(!is.na(x)))))
  return(se)
}