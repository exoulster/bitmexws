
options(scipen=10)

#' Initialize DB and trade related tables
#' @import dplyr dbplyr RPostgres DBI
#' @export
init_trade = function(conn) {
  if (dbExistsTable(conn, 'meta_trade')) db_drop_table(conn, 'meta_trade')
  if (dbExistsTable(conn, 'trade')) db_drop_table(conn, 'trade')
  db_create_table(conn, 'meta_trade', types=c(
    'filename'='TEXT',
    'date'='DATE',
    'imported'='BOOLEAN'
  ), temporary=FALSE)
  db_create_table(conn, 'trade', types=c(
    'timestamp'='TIMESTAMPTZ',
    'symbol'='TEXT',
    'side'='TEXT',
    'size'='INTEGER',
    'price'='REAL',
    'tickDirection'='TEXT',
    'trdMatchID'='TEXT',
    'grossValue'='REAL',
    'homeNotional'='REAL',
    'foreignNotional'='INTEGER'
  ), temporary=FALSE)
  db_create_index(conn, 'trade', 'trdMatchID', unique=TRUE)
  db_create_index(conn, 'trade', 'timestamp')
}


#' Initialize DB and orderBookL2 tables
#' @import dplyr dbplyr RPostgres DBI
#' @export
init_orderBookL2 = function(conn) {
  if (dbExistsTable(conn, 'meta_orderBookL2')) dplyr::db_drop_table(conn, 'meta_orderBookL2')
  if (dbExistsTable(conn, 'orderBookL2')) dplyr::db_drop_table(conn, 'orderBookL2')
  dplyr::db_create_table(conn, 'meta_orderBookL2', types=c(
    'filename'='TEXT',
    'date'='DATE',
    'imported'='BOOLEAN'
  ), temporary=FALSE)
  dplyr::db_create_table(conn, 'orderBookL2', types=c(
    'symbol'='TEXT',
    'id'='TEXT',
    'side'='TEXT',
    'price'='REAL',
    'size'='REAL',
    'action'='TEXT',
    'start_time'='TEXT',
    'end_time'='TEXT'
  ), temporary=FALSE)
  dplyr::db_create_index(conn, 'meta_orderBookL2', 'filename', unique=TRUE)
  dplyr::db_create_index(conn, 'orderBookL2', 'start_time')
  dplyr::db_create_index(conn, 'orderBookL2', 'end_time')
}


#' Import trade into DB
#' @import dplyr dbplyr RPostgres DBI
#' @export
import_trade = function(conn, filenames, full=FALSE) {
  if (full) init_trade(conn)

  if (dbExistsTable(conn, 'meta_trade')) {
    imported = tbl(conn, 'meta_trade') %>%
      filter(imported==TRUE) %>%
      pull(filename)
  } else {
    imported = ''
  }

  if (length(imported) > 0) {
    message(paste(length(imported), 'files already imported previously, ignored this time'))
  }
  lapply(filenames[!filenames %in% imported], function(fn) {
    trade = read_trade(fn)
    dbWriteTable(conn, 'trade', trade, append=TRUE, copy=TRUE)
    message(paste('Imported', fn))
  })
  message(paste('Total count', tbl(conn, 'trade') %>% count() %>% pull()))

  meta_trade = tibble(
    filename = filenames,
    date = lubridate::today(),
    imported = TRUE
  )
  dbWriteTable(conn, 'meta_trade', meta_trade, append=TRUE, copy=TRUE)
}



insert_orderBookL2 = function(conn, scd) {
  dbWriteTable(conn, name='orderBookL2', value=as.data.frame(scd), append=TRUE, temporary=FALSE)
}

update_orderBookL2 = function(conn, scd) {
  query = 'update "orderBookL2" set end_time = $1
    where end_time is null and symbol=$2 and id=$3 and side=$4'
  lapply(1:nrow(scd), function(i) {
    row = scd[i,]
    params = list(row[['start_time']], row[['symbol']], row[['id']], row[['side']])
    res = dbSendStatement(conn, query, params)
    ra = dbGetRowsAffected(res)
    dbClearResult(res)
    ra
  })
}

#' Import orderBookL2 data into DB as SCD table
#' @import dplyr dbplyr RPostgres DBI
#' @export
import_orderBookL2 = function(conn, filenames, full=FALSE, info=FALSE) {
  if (full) init_orderBookL2(conn)

  if (dbExistsTable(conn, 'meta_orderBookL2')) {
    imported = tbl(conn, 'meta_orderBookL2') %>%
      dplyr::filter(imported==TRUE) %>%
      pull(filename)
  } else {
    imported = ''
  }

  if (length(imported) > 0) {
    message(paste(length(imported), 'files already imported previously, ignored this time'))
  }


  lapply(filenames[!filenames %in% imported], function(fn) {
    msgs = bitmexws::read_msgs(fn)
    if (length(msgs) == 0) return(NULL)

    scd.origin = tbl(conn, 'orderBookL2') %>% filter(is.na(end_time)) %>% collect() %>% as_scd()
    scd.increment = merge_scd(msgs)
    lst = merge_scd(scd.origin, scd.increment)

    update_orderBookL2(conn, lst$to_update)
    insert_orderBookL2(conn, lst$to_insert)
    message(paste('Imported', fn))
  })

  meta_orderBookL2 = tibble::tibble(
    filename = filenames[!filenames %in% imported],
    date = lubridate::today(),
    imported = TRUE
  )
  dbWriteTable(conn, 'meta_orderBookL2', meta_orderBookL2, append=TRUE, copy=TRUE)
}


