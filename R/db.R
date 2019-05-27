

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
  ))
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
  ))
  db_create_index(conn, 'trade', 'trdMatchID', unique=TRUE)
  db_create_index(conn, 'trade', 'timestamp')
}


#' Initialize DB and orderBookL2 tables
#' @import dplyr dbplyr RPostgres DBI
#' @export
init_orderBookL2 = function(conn) {
  if (dbExistsTable(conn, 'meta_orderBookL2')) dplyr::db_drop_table(conn, 'meta_orderBookL2')
  if (dbExistsTable(conn, 'orderBookL2_minute')) dplyr::db_drop_table(conn, 'orderBookL2_minute')
  dplyr::db_create_table(conn, 'meta_orderBookL2', types=c(
    'filename'='TEXT',
    'date'='DATE',
    'imported'='BOOLEAN',
    'timestamp'='TIMESTAMPTZ'
  ))
  dplyr::db_create_table(conn, 'orderBookL2_minute', types=c(
    'timestamp'='TIMESTAMPTZ',
    'table'='TEXT',
    'action'='TEXT',
    'data'='JSONB'
  ))
  dplyr::db_create_index(conn, 'meta_orderBookL2', 'filename', unique=TRUE)
  dplyr::db_create_index(conn, 'meta_orderBookL2', 'timestamp', unique=TRUE)
  dplyr::db_create_index(conn, 'orderBookL2_minute', 'timestamp', unique=TRUE)
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


#' Import orderBookL2 data into DB
#' @import dplyr dbplyr RPostgres DBI
#' @export
import_orderBookL2 = function(conn, filenames, full=FALSE) {
  if (full) init_orderBookL2(conn)

  if (dbExistsTable(conn, 'meta_orderBookL2')) {
    imported = tbl(conn, 'meta_orderBookL2') %>%
      dplyr::filter(imported==TRUE) %>%
      pull(filename)
  } else {
    imported = ''
  }

  msgs.update = lapply(filenames, function(fn) {
    msgs = bitmexws::read_msgs(fn)
    bitmexws::reduce_msgs(msgs, strict=FALSE)
  })
  msgs.partial = bitmexws::accumulate_msgs(msgs.update, strict=FALSE)
  msg.dfr = lapply(msgs.partial, function(msg) {
    msg$data = msg$data %>% to_json()
    as_tibble(msg[attr(msg, 'names')])
  }) %>% bind_rows()
  dbWriteTable(conn, 'orderBookL2_minute', msg.dfr, append=TRUE, copy=TRUE)

  meta_orderBookL2 = tibble::tibble(
    filename = filenames,
    date = lubridate::today(),
    imported = TRUE,
    timestamp = msg.dfr$timestamp
  )
  dbWriteTable(conn, 'meta_orderBookL2', meta_orderBookL2, append=TRUE, copy=TRUE)
}
