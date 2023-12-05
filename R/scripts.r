#' Function to clear data
#'
#' @param1 Connection to postgres
#' 
#' @param2 imput ed
#'
#' @return the completed query.
#'
#'@example
#'clear_ed(conn, '00-000-00')
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


     all_comp_vr_raw <- subset(fullvr_mics, ed == e_update)

     all_comp_vr_to_process <- all_comp_vr_raw %>%
       distinct(interview__key, .keep_all = TRUE) %>%
       filter(responsible1.x != 'DELETION_VR23')


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
     dbExecute(c, update_queries_test[[u]])
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

