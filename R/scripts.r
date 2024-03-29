#' Function to clear some ed data for specific columns from PostgreSQL.
#'
#' @param C connection to PostgreSQL Database.
#' 
#' @param e imput ed.
#' 
#' @param t imput table name to clear.
#'
#' @return Completed is returned if successful.
#'
#'@example
#'clear_ed(conn, '51-001-00', 'tablename')
#' @export
#' 
clear_ed<- function(c, e, t) {
    conn2 <- c
    t <- t
    e <- e
    for (s in 1:length(e)) {
      ed_no <- e[[s]]
      query<- paste0("UPDATE ",t," SET photovalid = NULL,
      photo_cnt = NULL, living_quarter = NULL, bldg_refn = bldg_number,
      bldg_newn = NULL,
      isbldg = NULL,
      population = NULL,
      tot_hh_number = NULL,
      interview__key = NULL,
      int_bldg_desc = NULL WHERE ed_2023 = '", ed_no ,"'; ")
 
      DBI::dbExecute(conn2, query)

      cat(paste0("ED ", ed_no ," cleared. \n \n"))
 
    }
 return(print('Completed.'))
}

#' Function to assign interview key from MySql to PostgreSQL.
#'
#' @param c Connection to PostgreSQL database.
#' 
#' @param e imput ed.
#'
#' @return Returns the completed query and checks if there are errors.
#'
#'@example
#'clear_ed(conn, '41-002-00')
#' @export

assign_interview__keys<- function (c, e) {

  for (s in 1:length(e)) {
     e_update <- e[[s]]

     query_clear<- paste0("UPDATE sde.mics7_building SET photo_cnt = NULL,
     living_quarter = NULL, bldg_refn = bldg_number,
     bldg_newn = NULL,
     isbldg = NULL,
     population = NULL,
     tot_hh_number = NULL,
     interview__key = NULL,
     int_bldg_desc = NULL WHERE ed_2023 = '", e_update,"'; ")
 
     DBI::dbExecute(c, query_clear)

     query_fvr <- "SELECT * FROM fullVR_mics"
     fvr<- dbGetQuery(db, query_fvr)

     if ("Orange Walk" %in% fvr$district) {
      fvr$district[fvr$district == "Orange Walk"] <- "2"
      }
     if ("Belize" %in% fvr$district) {
      fvr$district[fvr$district == "Belize"] <- "3"
      }
     if ("Corozal" %in% fvr$district) {
      fvr$district[fvr$district == "Corozal"] <- "1"
      }
     if ("Cayo" %in% fvr$district) {
      fvr$district[fvr$district == "Cayo"] <- "4"
      }
     if ("Stann Creek" %in% fvr$district) {
      fvr$district[fvr$district == "Stann Creek"] <- "5"
      }
     if ("Toledo" %in% fvr$district) {
      fvr$district[fvr$district == "Toledo"] <- "6"
      }
     if ("1" %in% fvr$living_quarter) {
      fvr$living_quarter[fvr$living_quarter == "1"] <- "Yes"
      }
     if ("2" %in% fvr$living_quarter) {
      fvr$living_quarter[fvr$living_quarter == "2"] <- "No"
      }

    fvr$blk_uid <- paste0(fvr$district,"-",fvr$ed,"-",fvr$block,"-",fvr$buildingID)


    all_comp_vr_raw <- subset(fvr, ed == e_update)

    all_comp_vr_to_process <- dplyr::distinct(all_comp_vr_raw, interview__key, .keep_all = TRUE)
    all_comp_vr_to_process <- dplyr::filter(all_comp_vr_to_process, responsible1.x != 'DELETION_VR23')


     update_queries_test <- character(nrow(all_comp_vr_to_process))

     for (i in seq_len(nrow(all_comp_vr_to_process))) {
       interview_key <- all_comp_vr_to_process$interview__key[i]
       living_quarter <- all_comp_vr_to_process$living_quarter[i]
       description <- gsub("'", "''", all_comp_vr_to_process$description[i])
       is_building <- all_comp_vr_to_process$isBuilding[i]
       blk_uid <- all_comp_vr_to_process$blk_uid[i]

      update_queries_test[i] <- paste0("UPDATE sde.mics7_building SET interview__key = '", interview_key,
                                   "', living_quarter = '", living_quarter,
                                   "', int_bldg_desc = '", description,
                                   "', isbldg = '", is_building,
                                   "' WHERE mics7_building.blk_uid = '", blk_uid, "';")
     }
     for (u in 1:length(update_queries_test)) {
     DBI::dbExecute(c, update_queries_test[[u]])
       }

     query_building<- paste0("SELECT * FROM sde.mics7_building")
     check_buildings<- sf::st_read(c, query = query_building)
     check_buildings <- check_buildings[check_buildings$ed_2023 == e_update, ]
     null_values <- is.na(check_buildings$interview__key)
     num_null_values <- sum(null_values)

     if (num_null_values > 0) {
        cat(paste0("ED ",e_update," has some missing interview__key. \n"))
        subset_buildings <- subset(check_buildings, is.na(interview__key))
        cat(paste0("blk_uid ",subset_buildings$blk_uid," has some missing interview__key. \n"))
     } else {
      cat(paste0("ED ",e_update," has no missing interview__key. \n"))
      }

  }
return(print("Completed."))
}

#' Function to get visitation records in csv and compare it to the spatail data.
#'
#'  
#' @param imput ed in a list.
#' 
#' @param location Input location for where the files will be stored.
#'
#' @returns Returns a csv, the completed query, and a list of counts.
#'
#'@example
#'get_vr(list('54-444-44'), '\\directory\\')
#' @export

get_vr <- function(ed_list, location) {

  spat_building<- sf::st_read(conn, query = "Select * from sde.mics7_building")


  
  ed_hhs<- data.frame()
  raw_vrs<- data.frame()
  vrs<- data.frame()
  filter_vrs<- data.frame()
  spat_building_process<- data.frame()
  for(e in 1:length(ed_list)) {
    
    
    ed_no <- ed_list[[e]]

    spat_building_filter <- subset(spat_building, ed_2023 == ed_no)
    spat_building_filter <- sf::st_drop_geometry(spat_building_filter)
    
    query_fvr <- "SELECT * FROM fullVR_mics"
    fvr<- dbGetQuery(db, query_fvr)

    if ("Orange Walk" %in% fvr$district) {
      fvr$district[fvr$district == "Orange Walk"] <- "2"
      }
    if ("Belize" %in% fvr$district) {
      fvr$district[fvr$district == "Belize"] <- "3"
      }
    if ("Corozal" %in% fvr$district) {
      fvr$district[fvr$district == "Corozal"] <- "1"
      }
    if ("Cayo" %in% fvr$district) {
      fvr$district[fvr$district == "Cayo"] <- "4"
      }
    if ("Stann Creek" %in% fvr$district) {
     fvr$district[fvr$district == "Stann Creek"] <- "5"
      }
    if ("Toledo" %in% fvr$district) {
     fvr$district[fvr$district == "Toledo"] <- "6"
      }
    if ("1" %in% fvr$living_quarter) {
     fvr$living_quarter[fvr$living_quarter == "1"] <- "Yes"
      }
    if ("2" %in% fvr$living_quarter) {
      fvr$living_quarter[fvr$living_quarter == "2"] <- "No"
      }
   
   fvr$blk_uid <- paste0(fvr$district,"-",fvr$ed,"-",fvr$block,"-",fvr$buildingID)
    
    all_comp_vr_raw <- subset(fvr, ed == ed_no)
    
    query_vri <- "SELECT * FROM vr_inconsistencies"
    vri<- dbGetQuery(db, query_vri)
    vr_inconsistencies_filter <- subset(vri, ed == ed_no)
    
    i_hhs <- dplyr::tally(dplyr::group_by(all_comp_vr_raw, interview__key, hhs_roster__id))
    
    i_hhs2<- na.omit(i_hhs)
    
    i_hhs3<- aggregate(i_hhs2$n, list(i_hhs2$interview__key), FUN=sum)
    names(i_hhs3)[names(i_hhs3) == 'Group.1'] <- 'interview__key'
    names(i_hhs3)[names(i_hhs3) == 'x'] <- 'hhs_count'
    
    total_hhs<- as.data.frame(sum(i_hhs3$hhs_count))
    
    all_comp_vr_to_process <- dplyr::distinct(all_comp_vr_raw, interview__key, .keep_all = TRUE)
    all_comp_vr_to_process <- dplyr::filter(all_comp_vr_to_process, responsible1.x != 'DELETION_VR23')
    
    filter_all_comp_vr_to_process <- dplyr::filter(all_comp_vr_to_process, isBuilding == 'Yes - Main Building' | living_quarter == 'Yes')
    
    ed_hhs<- rbind(ed_hhs, i_hhs3)
    raw_vrs<- rbind(raw_vrs, all_comp_vr_raw)
    vrs <- rbind(vrs,all_comp_vr_to_process)
    filter_vrs<- rbind(filter_vrs,filter_all_comp_vr_to_process)
    spat_building_process<-rbind(spat_building_process, spat_building_filter)
    
    
    write.csv(all_comp_vr_to_process, paste0(location, "ED-",ed_no,"_mics7_vrs.csv"), row.names = FALSE)
    write.csv(filter_all_comp_vr_to_process, paste0(location, "ED-",ed_no,"_mics7__isBldgLivingQ_vrs.csv"), row.names = FALSE)
    write.csv(i_hhs3, paste0(location, "ED-",ed_no,"_mics7_hhs_count_by_interview__key.csv"), row.names = FALSE)
    
    cat(paste0("All mics7 vr building count for ED ",ed_no," is ",nrow(all_comp_vr_to_process),".\n"))
    cat(paste0("All mics7 vr isbuilding and living quarter count for ED ",ed_no," is ",nrow(filter_all_comp_vr_to_process), ".\n"))
    cat(paste0("ED ",ed_no," has ", nrow(vr_inconsistencies_filter)," errors! \n"))
    cat(paste0("All spatial building count ED ", ed_no, " is ", nrow(spat_building_filter), ". \n"))
    cat(paste0("Total households count for ED ",ed_no," is ",total_hhs, ". \n \n"))
  }
  
  location <- location
  cat(paste0("All raw vrs for all EDs are ", nrow(raw_vrs), ".\n"))
  cat(paste0("All spatial buildings for all EDs are ", nrow(spat_building_process), ".\n"))
  cat(paste0("All mics_vr buildings for all EDs are ", nrow(vrs), ".\n"))
  write.csv(vrs, paste0(location, "1. all_mics7_vrs.csv"), row.names = FALSE)
  write.csv(filter_vrs, paste0(location, "2. all_mics7_filtered_isBldg_LivingQ_vrs.csv"), row.names = FALSE)
  write.csv(raw_vrs, paste0(location, "0. all_mics7_raw_vrs.csv"), row.names = FALSE)
  return(print("Completed."))
}

#' Function to sort spatial blocks and buildings and check if they are in sequence.
#'
#'  
#' @param ed imput ed. Make it a list if you have multiple ed.
#' 
#' @param location input location.
#'
#' @returns Returns the completed query, and a list of counts.
#'
#'@example
#'sequence_check(list('54-444-44'), '\\directory\\')
#' @export

sequence_check<- function(ed_list, location) {
  
  spat_building <- sf::st_transform(sf::st_zm(sf::st_read(conn, query = "SELECT * FROM mics7_building"), drop = TRUE, what = "ZM"), 4326)

  spat_blocks <- sf::st_transform(sf::st_zm(sf::st_read(conn, query = "SELECT * FROM mics7_blocks"), drop = TRUE, what = "ZM"), 4326)

  spat_ed <- sf::st_transform(sf::st_zm(sf::st_read(conn, query = "SELECT * FROM mics7_ed"), drop = TRUE, what = "ZM"), 4326)

  
  checkALL<- data.frame()
  for(e in 1:length(ed_list)) {
    
    ed_no <- ed_list[[e]]
    
    e_ed<- subset(spat_ed, spat_ed$ed_2023 == ed_no)

    e_blocks<-sf::st_join(spat_blocks, e_ed, left = FALSE)
    
    #Check sequence of blocks
    e_blocks_check <- subset(spat_blocks, spat_blocks$ed_2023 == ed_no)
    e_blocks_check$blk_newn_2023 <- as.numeric(e_blocks_check$blk_newn_2023)
    e_blocks_check<- e_blocks_check[order(e_blocks_check$blk_newn_2023),]
    e_blocks_check$seq_chk<-1:nrow(e_blocks_check)
    e_blocks_check$match<- ifelse(e_blocks_check$blk_newn_2023==e_blocks_check$seq_chk,"Yes","No")
    
    count_no <- sum(e_blocks_check$match == "No")
    
    if(count_no >0 ) { 
      cat(paste0("ED ",ed_no," have blocks out of sequence. \n"))
      subset_e_e_blocks_check <- subset(e_blocks_check, e_blocks_check$match == "No")
      cat(paste0(subset_e_e_blocks_check$blk_newn_2023," is out of sequence. \n \n"))
    } else {
      cat(paste0("ED ",ed_no," have no blocks out of sequence. \n \n"))
    }
    
    #use the layers from the joins
    #e_building<- sf::st_join(spat_building, e_blocks, left = FALSE)
    #e_building_use <- subset (e_building, select=c(cluster, ed_2023, blk_newn_2023.y,bldg_newn, bldg_uid, blk_uid.x, isbldg, living_quarter, interview__key))
    
    e_building_use <- subset(spat_building, ed_2023 == ed_no)
    e_building_use <-sf::st_drop_geometry(e_building_use)
    
    e_building_use <- subset(e_building_use, select=c(cluster, ed_2023, blk_newn_2023,bldg_newn, bldg_uid, blk_uid, isbldg, living_quarter, interview__key))
    
    #Check empty values of building
    null_value <- is.na(e_building_use$interview__key)
    num_null_int<- sum(null_value)
    
    if(num_null_int > 0) {
      cat(paste0("ED ",ed_no," has empty interview__key. \n"))
      subset_e_building_use <- subset(e_building_use, is.na(interview__key))
      cat(paste0(subset_e_building_use$blk_uid," is empty interview__key. \n \n"))
    } else {
      cat(paste0("ED ", ed_no, " has no missing interview__key. \n \n"))
    }
    
    e_building_use_check <- dplyr::filter(e_building_use, isbldg == 'Yes - Part of a building' | isbldg == 'Not a building' & living_quarter == 'No')
    
    non_null <- !is.na(e_building_use_check$bldg_newn)
    num_non_null_values <- sum(non_null)
    
    
    if (num_non_null_values > 0) {
      cat(paste0("ED ",ed_no," has bldg_newn where it shouldn't. \n"))
      subset_e_building_use_check <- subset(e_building_use_check, !is.na(bldg_newn))
      cat(paste0("blk_uid ",subset_e_building_use_check$blk_uid," has some missing interview__key. \n \n"))
    } else {
      cat(paste0("ED ",ed_no," has no bldg_newn in Yes - Part of Building or Not a building with no living quarters. \n \n"))
    }
    
    e_building_use_filter <- dplyr::filter(e_building_use, isbldg == 'Yes - Main Building' | living_quarter == 'Yes')

    e_building_use_filter$blk_newn_2023 <- as.numeric(e_building_use_filter$blk_newn_2023)
    e_building_use_filter$bldg_newn<- as.numeric(e_building_use_filter$bldg_newn)
    e_building_use_filter<- e_building_use_filter[order(e_building_use_filter$blk_newn_2023,e_building_use_filter$bldg_newn ),]
    
    e_building_use_filter$seq_chk<-1:nrow(e_building_use_filter)

    all(e_building_use_filter$bldg_newn == e_building_use_filter$seq_chk)

    e_building_use_filter$match<- ifelse(e_building_use_filter$bldg_newn==e_building_use_filter$seq_chk,"Yes","No")
    
    checkDf<- subset(e_building_use_filter, e_building_use_filter$match =="No")
    
    checkDf_no<- checkDf
    checkDf_no <- sf::st_drop_geometry(checkDf_no)

    checkALL <- rbind(checkALL,checkDf_no)

  }
  countsofError<- as.data.frame(nrow(checkALL))
  cat(paste0("This(ese) ed(s) has(ve) blg_number(s) ", countsofError," out of sequence. \n"))
  write.csv(checkALL,paste0(location,"outSeq.csv"), row.names = TRUE)
  return(print("Completed."))
 }

#' Function to get the amount of completed eds and see which ones need to be added to mics7_dp_assignment table.
#'
#' @returns Returns the completed query, and a list of counts.
#'
#' @export
get_completed_ed<-function() {
  
  query_assignments<- "Select *, ed as ed_2023 from mics_ass_summary"
  completed_query<- dbGetQuery(db, query_assignments)

  completed_query$completed<- ifelse(completed_query$Assignments_Pending == 0, "True", "False")
  completed_query <- subset(completed_query, completed_query$completed == "True")

  query2<- "SELECT * FROM mics7_dp_assignments"
  mics7_dp_assignments <- dbGetQuery(conn, query2)

  noMatch <- dplyr::anti_join(completed_query, mics7_dp_assignments, by = "ed_2023")
  count_noMatch<- nrow(noMatch)

  if(count_noMatch != 0) {
  cat(paste0("Total EDs Completed: ", nrow(completed_query), "(", round((nrow(completed_query)/207*100), 1), "%). \n \n"))
  cat(paste0("'", noMatch$ed_2023, "',\n")) 
  } else {
  cat(paste0("Total EDs Completed: ", nrow(completed_query), "(", round((nrow(completed_query)/207*100), 1), "%). \n \n"))
  cat(paste0("No completed MICS7 EDs. \n"))
  return(print("Completed."))
  }
}

#' Function to AutoNumber buildings in the spatial database.
#'
#'  
#' @param c PostgreSQL Connection.
#' 
#' @param ed_list Single ED or a list of ED. 
#'
#' @returns Returns the completed query.
#'
#'@example
#'autonumber_ed(conn, list('54-444-44'))
#' @export

autonumber_ed<- function(c, ed_list) {
  
  spat_building <- sf::st_transform(sf::st_zm(sf::st_read(conn, query = "SELECT * FROM mics7_building"), drop = TRUE, what = "ZM"), 4326)
  
  for(e in 1:length(ed_list)) {
    
    ed_no <- ed_list[[e]]

    e_building_use <- subset(spat_building, ed_2023 == ed_no)
    e_building_use <-sf::st_drop_geometry(e_building_use)
    
    e_building_use <- subset(e_building_use, select=c(cluster, ed_2023, blk_newn_2023,bldg_number, bldg_uid, blk_uid, isbldg, living_quarter, interview__key))
    
    e_building_use_filter <- dplyr::filter(e_building_use, isbldg == 'Yes - Main Building' | living_quarter == 'Yes')
    
    e_building_use_filter$blk_newn_2023 <- as.numeric(e_building_use_filter$blk_newn_2023)
    e_building_use_filter$bldg_number<- as.numeric(e_building_use_filter$bldg_number)
    e_building_use_filter<- e_building_use_filter[order(e_building_use_filter$blk_newn_2023,e_building_use_filter$bldg_number),]
    
    e_building_use_filter$nnew <-1:nrow(e_building_use_filter)
    
    
    
    update_queries <- character(nrow(e_building_use_filter))
    
    for (i in seq_len(nrow(e_building_use_filter))) {
      bldg_newn <- e_building_use_filter$nnew[i]
      blk_uid <- e_building_use_filter$blk_uid[i]
      
      update_queries[i] <- paste0("UPDATE sde.mics7_building SET bldg_newn = '", bldg_newn,
                                       "' WHERE mics7_building.blk_uid = '", blk_uid, "';")
    }
    for (u in 1:length(update_queries)) {
      DBI::dbExecute(c, update_queries[[u]])
    }
   cat(paste0("ED ",ed_no," has been renumbered. \n"))
  }
  return("Completed.")
}


#' Function to Reconcile non spatial data to spatial database.
#'
#'  
#' @param c PostgreSQL Connection.
#' 
#' @param e Single ED or a list of ED. 
#'
#' @returns Returns the completed query.
#'
#'@example
#'reconcile_vr_spatial(conn, list('54-444-44'))
#' @export

reconcile_vr_spatial<-function(c, e) {
  spat_build<- st_read(conn, query = "SELECT objectid, district, ed_2023, blk_number, bldg_number, isbldg, living_quarter as spat_living_quart, interview__key, shape FROM mics7_building") %>% 
  st_zm(drop = T, what = "ZM") %>% 
  st_transform(4326)

  query1<- "SELECT district, ed, block, buildingID, isBuilding, living_quarter, `responsible1.x`, description, interview__key FROM fullVR_mics"

  fvr<- dbGetQuery(db, query1)


  if ("Orange Walk" %in% fvr$district) {
    fvr$district[fvr$district == "Orange Walk"] <- "2"
    }
  if ("Belize" %in% fvr$district) {
    fvr$district[fvr$district == "Belize"] <- "3"
    }
  if ("Corozal" %in% fvr$district) {
    fvr$district[fvr$district == "Corozal"] <- "1"
    }
  if ("Cayo" %in% fvr$district) {
    fvr$district[fvr$district == "Cayo"] <- "4"
    }
  if ("Stann Creek" %in% fvr$district) {
    fvr$district[fvr$district == "Stann Creek"] <- "5"
    }
  if ("Toledo" %in% fvr$district) {
      fvr$district[fvr$district == "Toledo"] <- "6"
    }
  if ("1" %in% fvr$living_quarter) {
      fvr$living_quarter[fvr$living_quarter == "1"] <- "Yes"
    }
  if ("2" %in% fvr$living_quarter) {
      fvr$living_quarter[fvr$living_quarter == "2"] <- "No"
    }

  fvr$blk_uid <- paste0(fvr$district,"-",fvr$ed,"-",fvr$block,"-",fvr$buildingID)

  all_fvr <- dplyr::distinct(fvr, interview__key, .keep_all = TRUE)
  all_fv <- dplyr::filter(all_fvr, responsible1.x != 'DELETION_VR23')

  all_build<- spat_build %>%
    st_drop_geometry()

  all_build$spat_living_quart[all_build$spat_living_quart == "NA"] <- "No"

  for(a in 1:length(exclude_int_key)) {
    b <- exclude_int_key[[a]]
    all_build<- all_build[all_build$interview__key != b, ]
  }

  for(c in 1:length(exclude_ed)) {
    d <- exclude_ed[[c]]
    all_build<- all_build[all_build$ed_2023 != d, ]
  }

  all_compare_df <- full_join(all_fvr, all_build, by = "interview__key")
  all_compare_df$living_quarter[is.na(all_compare_df$living_quarter)] <- "No"
  all_compare_df$isbuilding_result<- ifelse(all_compare_df$isBuilding == all_compare_df$isbldg, "T", "F")
  all_compare_df$livingq_result<- ifelse(all_compare_df$living_quarter == all_compare_df$spat_living_quart,"T","F")

  all_diff_table<- subset(all_compare_df, all_compare_df$isbuilding_result == "F" | all_compare_df$livingq_result == "F")

  for (o in 1:length(e)) {
    ed_no <- e[[o]]

  all_comp_vr_raw <- subset(fvr, ed == ed_no)

  all_comp_vr_to_process <- dplyr::distinct(all_comp_vr_raw, interview__key, .keep_all = TRUE)
  all_comp_vr_to_process <- dplyr::filter(all_comp_vr_to_process, responsible1.x != 'DELETION_VR23')

  r_build<- subset(spat_build, spat_build$ed_2023 == ed_no)
  r_build<- r_build %>%
    st_drop_geometry()

  compare_df <- full_join(all_comp_vr_to_process, r_build, by = "interview__key")  # full_join includes all rows from both data frames

  # Replace NULL with NA in the data frame
  compare_df$living_quarter[is.na(compare_df$living_quarter)] <- "NA"

  compare_df$isbuilding_result <- ifelse(compare_df$isBuilding == compare_df$isbldg, "T", "F")
  compare_df$livingq_result <- ifelse(compare_df$living_quarter == compare_df$spat_living_quart, "T", "F")

  diff_table <- subset(compare_df, compare_df$isbuilding_result == "F" | compare_df$livingq_result == "F")

  if(nrow(diff_table) > 0)
  {

    cat(paste0("\nA total of: ",nrow(diff_table)," record(s) was(ere) updated/modified in the visitation records. \n"))
    cat(paste0("\nobjectid=",diff_table$objectid," Interview__key='",diff_table$interview__key,"' isBuilding match? ", diff_table$isbuilding_result, ". Living_quarter match? ", diff_table$livingq_result,". blk_uid='",diff_table$blk_uid,"'"))
  
  
  update_spat_query <- character(nrow(diff_table))

  for (i in seq_len(nrow(diff_table))) {
    interview__key <- diff_table$interview__key[i]
    living_quarter <- diff_table$living_quarter[i]
    description <- gsub("'", "''", diff_table$description[i])
    is_building <- diff_table$isBuilding[i]
    ed_2023 <- diff_table$ed[i]

    update_spat_query[i] <- paste0("UPDATE sde.mics7_building SET living_quarter = '", living_quarter,
                                     "', int_bldg_desc = '", description,
                                    "', isbldg = '", is_building,
                                     "' WHERE mics7_building.interview__key = '", interview__key, 
                                     "' AND mics7_building.ed_2023 = '",ed_2023,"';")
  }
  for (u in 1:length(update_spat_query)) {
   DBI::dbExecute(conn, update_spat_query[[u]])
   cat(paste0("\n",ed_no," has been updated."))
  }
  }else {
   cat(paste0("\n All Match in ",ed_no,"! \n"))
  }
  } 
  return("Completed.")
}

#' Function to Update interivew__id from MySQL table to PostgreSQL Table.
#'
#'  
#' @param c PostgreSQL Connection.
#' 
#' @param e Single ED or a list of ED. 
#'
#' @returns Returns the completed query.
#'
#'@example
#'reconcile_vr_spatial(conn, list('54-444-44'))
#' @export


update_interview__id<- function(c, e) {
  
  spat_build<- st_read(conn, query = "SELECT objectid, district, ed_2023, blk_number, bldg_number, isbldg, living_quarter as spat_living_quart, interview__key, shape FROM mics7_building") %>% 
    st_zm(drop = T, what = "ZM") %>% 
    st_transform(4326)
  
  query1<- "SELECT district, ed, block, buildingID, isBuilding, living_quarter, `responsible1.x`, description, interview__key, interview__id FROM fullVR_mics"

  fvr<- dbGetQuery(db, query1)
  
  
  if ("Orange Walk" %in% fvr$district) {
    fvr$district[fvr$district == "Orange Walk"] <- "2"
  }
  if ("Belize" %in% fvr$district) {
    fvr$district[fvr$district == "Belize"] <- "3"
  }
  if ("Corozal" %in% fvr$district) {
    fvr$district[fvr$district == "Corozal"] <- "1"
  }
  if ("Cayo" %in% fvr$district) {
    fvr$district[fvr$district == "Cayo"] <- "4"
  }
  if ("Stann Creek" %in% fvr$district) {
    fvr$district[fvr$district == "Stann Creek"] <- "5"
  }
  if ("Toledo" %in% fvr$district) {
    fvr$district[fvr$district == "Toledo"] <- "6"
  }
  if ("1" %in% fvr$living_quarter) {
    fvr$living_quarter[fvr$living_quarter == "1"] <- "Yes"
  }
  if ("2" %in% fvr$living_quarter) {
    fvr$living_quarter[fvr$living_quarter == "2"] <- "No"
  }
  
  fvr$blk_uid <- paste0(fvr$district,"-",fvr$ed,"-",fvr$block,"-",fvr$buildingID)
  
  all_fvr <- dplyr::distinct(fvr, interview__key, .keep_all = TRUE)
  all_fvr <- dplyr::filter(all_fvr, responsible1.x != 'DELETION_VR23')
  
  all_build<- sf::st_drop_geometry(spat_build)
  
  all_build$spat_living_quart[all_build$spat_living_quart == "NA"] <- "No"
  
    
  for(l in 1:length(e)) {
  
  ed_no <- e[[l]]
  
  ed_numb <- subset(all_fvr, all_fvr$ed == ed_no)
  
  update_spat_query <- character(nrow(ed_numb))
  
  for (i in seq_len(nrow(ed_numb))) {
    interview__id<- ed_numb$interview__id[i]
    interview__key<- ed_numb$interview__key[i]
    ed_2023<- ed_numb$ed[i]

    update_spat_query[i] <- paste0("UPDATE sde.mics7_building SET interview__id = '", interview__id,
                                   "' WHERE mics7_building.interview__key = '", interview__key, 
                                   "' AND mics7_building.ed_2023 = '",ed_2023,"';")
  }
  for (u in 1:length(update_spat_query)) {
    DBI::dbExecute(conn, update_spat_query[[u]])
  }
  cat(paste0("\n",ed_no," has been updated."))
  }
  return(cat("\nCompleted."))
}