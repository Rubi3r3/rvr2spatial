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
      e <- e[[s]]
 
      query<- paste0("UPDATE",t," SET photovalid = NULL,
      photo_cnt = NULL, living_quarter = NULL, bldg_refn = bldg_number,
      bldg_newn = NULL,
      isbldg = NULL,
      population = NULL,
      tot_hh_number = NULL,
      interview__key = NULL,
      int_bldg_desc = NULL WHERE ed_2023 = '", ed_update,"'; ")
 
      DBI::dbExecute(conn2, query)
        
 
    }
 return(print('Completed'))
}



