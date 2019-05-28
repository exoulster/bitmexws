context("test-db")

skip('skipping db')

library(RPostgres)
conn = dbConnect(RPostgres::Postgres(), dbname='bitmex')

library(pool)
conn = pool::dbPool(RPostgres::Postgres(), dbname='bitmex')

# Initialisation
init_trade(conn)
init_orderBookL2(conn)

# Import
filenames = dir('~/Downloads/trade', pattern='trade\\.txt.+', full.names=TRUE, recursive=TRUE)
filenames = dir('~/Downloads/orderBookL2', pattern='orderBookL2\\.txt.+', full.names=TRUE, recursive=TRUE)
filenames
import_trade(conn, filenames, full=TRUE)

init_orderBookL2(conn)
filename = 'data-raw/orderBookL2.txt'
msgs = read_msgs(filename)
scd = to_scd(msgs[[1]]$data)
dbWriteTable(conn, 'orderBookL2_SCD', scd, append=TRUE, copy=TRUE)

msg = msgs[[2]]

res = dbSendStatement(conn, 'update "orderBookL2_SCD" set end_date = $1 where key in ($2)',
            list(escape(Sys.time()), escape(names(msg[['data']]))))

params = toString(c("XBTUSD_8799284700_Buy", "XBTUSD_8799284650_Buy"))
params = "'XBTUSD_8799284700_Buy'"
params = list(c('XBTUSD_8799284650_Buy', 'XBTUSD_8799284700_Buy') %>% escape() %>% toString())
# dbplyr::translate_sql('select * from "orderBookL2_SCD" where key in ($1)', params)
query = 'select * from "orderBookL2_SCD" where key in ($1)'
# query = 'select * from "orderBookL2_SCD"'
# param = 'XBTUSD_8799284700_Buy'
res = dbSendQuery(conn, query, param)
dbFetch(res)
dbClearResult(res)

query = 'select * from "orderBookL2_SCD" where key = $1'
param = 'XBTUSD_8799284650_Buy'
res = dbSendStatement(conn, query, param)
dbFetch(res)
dbClearResult(res)

query = 'update "orderBookL2_SCD" set end_date = $1 where key = $2 and end_date is null'
param = list(Sys.time(), 'XBTUSD_8799284650_Buy')
res = dbSendStatement(conn, query, param)
dbFetch(res)
dbClearResult(res)


update_orderBookL2_SCD = function(conn, msg) {
  lapply(names(msg[['data']]), function(nm) {
    params = list(msg[['timestamp']], nm)
    params
    query = 'update "orderBookL2_SCD" set end_date = $1 where key = $2 and end_date is null'
    res = dbSendStatement(conn, query, params)
    ra = dbGetRowsAffected(res)
    dbClearResult(res)
    ra
  })
}
update_orderBookL2_SCD(conn, msg)

tbl(conn, 'orderBookL2_SCD')

# query = paste(
#   'update "orderBookL2_SCD" set end_date = ',
#   escape(msg[['timestamp']]),
#   'where end_date is null and key in',
#   escape(names(msg[['data']])))
# res = dbSendStatement(conn, query)
# dbClearResult(res)
tbl(conn, 'orderBookL2_SCD') %>% filter(key %in% c('XBTUSD_8799284700_Buy', 'XBTUSD_8799284650_Buy'))

# XBTUSD_8799284700_Buy
# XBTUSD_8799284650_Buy

import_orderBookL2_SCD(conn, filenames[1:2], full=TRUE)


test_that("multiplication works", {
  expect_equal(2 * 2, 4)
})
