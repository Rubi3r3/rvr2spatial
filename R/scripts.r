#' Function to clear data
#'
#' @param Connection to postgres
#' 
#' @param imput ed
#'
#' @return the completed query.
#'
#'@example
#'clear_ed(conn, '51-001-00')
#' @export
clear_ed<- function(c, e, t) {
    conn2 <- c
    t <- t
    for (s in 1:length(e)) {
      e <- e [[s]]
      query<- paste0("UPDATE ",t," SET photovalid = NULL,
      photo_cnt = NULL, living_quarter = NULL, bldg_refn = bldg_number,
      bldg_newn = NULL,
      isbldg = NULL,
      population = NULL,
      tot_hh_number = NULL,
      interview__key = NULL,
      int_bldg_desc = NULL WHERE ed_2023 = '", e ,"'; ")
 
      DBI::dbExecute(conn2, query)
        
 
    }
 return(print('Completed.'))
}

#' Function to assign interview key to spatial data
#'
#' @param Connection to postgres
#' 
#' @param imput ed
#'
#' @return the completed query and checks if there are errors.
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

     all_comp_vr_to_process <- dplyr::all_comp_vr_raw %>%
       dplyr::distinct(interview__key, .keep_all = TRUE) 

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
     check_buildings<- st_read(c, query = query_building)
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
}

#' Function to get visitation records and compare it to the spatail data.
#'
#'  
#' @param imput ed
#' 
#' @param input location
#'
#' @returns the completed query, and a list of counts.
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
    
    filter_all_comp_vr_to_process <- filter(all_comp_vr_to_process, isBuilding == 'Yes - Main Building' | living_quarter == 'Yes')
    
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

#' Function to sort spatial blocks and buildings and check if they are in sequence
#'
#'  
#' @param imput ed. Make it a list if you have multiple ed.
#' 
#' @param input location
#'
#' @returns the completed query, and a list of counts.
#'
#'@example
#'sequence_check(list('54-444-44'), '\\directory\\')
#' @export

sequence_check<- function(ed_list, location) {
  
  spat_building<- st_read(conn, query = "SELECT * FROM mics7_building") %>% 
    st_zm(drop = T, what = "ZM") %>% 
    st_transform(4326)
  
  spat_blocks<- st_read(conn, query = "SELECT * FROM mics7_blocks") %>% 
    st_zm(drop = T, what = "ZM") %>% 
    st_transform(4326)
  
  spat_ed<- st_read(conn, query = "SELECT * FROM mics7_ed") %>% 
    st_zm(drop = T, what = "ZM") %>% 
    st_transform(4326)
  
  checkALL<- data.frame()
  for(e in 1:length(ed_list)) {
    
    ed_no <- ed_list[[e]]
    
    e_ed<- subset(spat_ed, spat_ed$ed_2023 == ed_no)

    e_blocks<-st_join(spat_blocks, e_ed, left = FALSE)
    
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
    #e_building<- st_join(spat_building, e_blocks, left = FALSE)
    #e_building_use <- subset (e_building, select=c(cluster, ed_2023, blk_newn_2023.y,bldg_newn, bldg_uid, blk_uid.x, isbldg, living_quarter, interview__key))
    
    e_building_use <- subset(spat_building, ed_2023 == ed_no) %>%
      st_drop_geometry()
    
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
    
    e_building_use_check <- filter(e_building_use, isbldg == 'Yes - Part of a building' | isbldg == 'Not a building' & living_quarter == 'No')
    
    non_null <- !is.na(e_building_use_check$bldg_newn)
    num_non_null_values <- sum(non_null)
    
    
    if (num_non_null_values > 0) {
      cat(paste0("ED ",ed_no," has bldg_newn where it shouldn't. \n"))
      subset_e_building_use_check <- subset(e_building_use_check, !is.na(bldg_newn))
      cat(paste0("blk_uid ",subset_e_building_use_check$blk_uid," has some missing interview__key. \n \n"))
    } else {
      cat(paste0("ED ",ed_no," has no bldg_newn in Yes - Part of Building or Not a building with no living quarters. \n \n"))
    }
    
    e_building_use_filter <- filter(e_building_use, isbldg == 'Yes - Main Building' | living_quarter == 'Yes')

    e_building_use_filter$blk_newn_2023 <- as.numeric(e_building_use_filter$blk_newn_2023)
    e_building_use_filter$bldg_newn<- as.numeric(e_building_use_filter$bldg_newn)
    e_building_use_filter<- e_building_use_filter[order(e_building_use_filter$blk_newn_2023,e_building_use_filter$bldg_newn ),]
    
    e_building_use_filter$seq_chk<-1:nrow(e_building_use_filter)

    all(e_building_use_filter$bldg_newn == e_building_use_filter$seq_chk)

    e_building_use_filter$match<- ifelse(e_building_use_filter$bldg_newn==e_building_use_filter$seq_chk,"Yes","No")
    
    checkDf<- subset(e_building_use_filter, e_building_use_filter$match =="No")
    
    checkDf_no<- checkDf %>% st_drop_geometry()
    
    checkALL <- rbind(checkALL,checkDf_no)

  }
  countsofError<- as.data.frame(nrow(checkALL))
  cat(paste0("This(ese) ed(s) has(ve) blg_number(s) ", countsofError," out of sequence. \n"))
  write.csv(checkALL,paste0(location,"outSeq.csv"), row.names = TRUE)
  return(print("Completed."))
 }



