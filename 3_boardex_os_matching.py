# Created by Yixin Sun on September 21, 2021
# This code uses the standardized names from OpenSecrets and standardized names from 
# BoardEx to match board members to campaign finance contributors

'''library(tidyverse)
library(stringdist)
library(furrr)
library(tictoc)
library(parallel)
library(data.table)
library(writexl)
library(knitr)
library(readxl)
library(lubridate)


ddir <- "/home/ysun9/OpenSecrets"
gdir <- file.path(ddir, "generated_data")

start <- Sys.time()


os_std <- 
  readRDS(file.path(gdir, "open_secrets/opensecrets_names_std.RDS"))  %>%
  mutate(LastName1 = str_sub(LastName, 1, 1), 
         Firm1 = paste(str_split(Firm_std, " ")[[1]][1], collapse = " ")) %>%
  select(-LastName, -FirstName)


boardex_std <- 
  readRDS(file.path(gdir, "intermediate/boardex_std.RDS")) %>% 
  mutate(DateStartRole = ymd(DateStartRole), 
         DateEndRole = ymd(DateEndRole), 
         LastName1 = str_sub(LastName, 1, 1), 
         Firm1 = paste(str_split(CompanyName_std, " ")[[1]][1], collapse = " ")) %>%
  mutate(DateStartRole = if_else(is.na(DateStartRole), ymd(19000101), DateStartRole), 
         DateEndRole = if_else(is.na(DateEndRole), ymd(29990101), DateEndRole)) 


# ===========================================================================
# Fuzzy matching on first name, last name, and firm name
# ===========================================================================
# function that first subsets the os data to relevant rows, and then 
# conducts jaro-winkler distance on first name, last name, and firm name
# We only want to categorize potential matches as ones that meet the following
# criteria
  # 1. First letter of the last name matches
  # 2. Contribution year falls within when the board member was active - TBD DEPENDING ON WHAT THE DATESTARTROLE COLUMN MEANS
fuzzymatch <- function(df, jw_thresh = .25, tomatch = select(os_std, -Firm), scale= .05){
  potentials <- tomatch[LastName1 == df$LastName1,]
  #potentials <- potentials[year(df$DateStartRole) - 2 <= Cycle & year(df$DateEndRole) + 2 >= Cycle,]
  potentials <- potentials[, `:=`(firm_fuzzy = stringdist(df$CompanyName_std, Firm_std, method = "jw", p = scale), 
                                  last_fuzzy = stringdist(df$Last_std, Last_std, method = "jw", p = scale), 
                                  first_fuzzy = stringdist(df$First_std, First_std, method = "jw", p = scale), 
                                  firm_word1 = Firm1 == df$Firm1)]

  # only keep matches under a specific match score threshold 
  potentials <- potentials[(firm_fuzzy <= jw_thresh | firm_word1) & 
                             (last_fuzzy <= jw_thresh) &  
                             (first_fuzzy <= jw_thresh)]
  
  potentials <- potentials[, c("FirmID", "Contrib", "ContribID", 
                               "Cycle", "firm_fuzzy","last_fuzzy", "first_fuzzy", 
                               "firm_word1")]
  potentials <- potentials[, `:=`(CompanyID = df$CompanyID, 
                                  CompanyName = df$CompanyName, 
                                  DirectorName = df$DirectorName, 
                                  DirectorID = df$DirectorID, 
                                  DateStartRole = df$DateStartRole, 
                                  DateEndRole = df$DateEndRole)]
  return(potentials)
}

#plan(future::multisession, workers = 4)
#options(future.globals.maxSize= 2*10^10)

tic()
fuzzy_matches <- 
  split(boardex_std, seq(nrow(boardex_std))) %>%
  #future_map_dfr(fuzzymatch) %>%
  map_df(fuzzymatch) %>%
  left_join(distinct(select(os_std, FirmID, Firm))) %>%
  select(bdx_Firm = CompanyName,
         bdx_Name = DirectorName,
         os_Firm = Firm,
         os_Name = Contrib, 
         os_FirmID = FirmID, 
         os_ContribID = ContribID, 
         os_Cycle = Cycle, 
         bdx_CompanyID = CompanyID, 
         bdx_DirectorID = DirectorID, 
         bdx_DateStartRole = DateStartRole, 
         bdx_DateEndRole = DateEndRole, 
         contains("fuzzy"), firm_word1)
toc()

write_xlsx(fuzzy_matches, file.path(gdir, "boardex_training/boardex_os_matches.xlsx"))


# For the training set
# 1. take out the "exact matches", which we define as 
  # either perfect match between first name, last name, and firm name, or REALLY
  # close match
# 2. Make sure matches are unique at the os_FirmID, os_ContribID, bdx_CompanyID, 
  # bdx_DirectorID level - there can be duplicates because of differences in ho
  # names are spelled 
# 3. Select relevant columns 
training_set <-
  fuzzy_matches %>%
  filter(!(first_fuzzy < .05 & last_fuzzy < .05 & firm_fuzzy < .05)) %>%
  select(-contains("fuzzy"), -firm_word1)


write_xlsx(training_set, file.path(gdir, "boardex_training/boardex_training.xlsx"))


print(paste("Matching Done In: ", difftime(Sys.time(), start)))'''

