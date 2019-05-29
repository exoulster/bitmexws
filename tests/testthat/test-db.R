context("test-db")

# skip('skipping db')

library(RPostgres)
conn = dbConnect(RPostgres::Postgres(), dbname='bitmex')


setwd('../../')
filename = 'data-raw/orderBookL2.txt'

msgs = read_msgs(filename)
msg.partial = msgs[[1]]
msg.update = msgs[[2]]
msg.delete = msgs[[38]]
msg.insert = msgs[[74]]
inc.partial = msg.partial$data
inc.update = msg.update$data
inc.delete = msg.delete$data
inc.insert = msg.insert$date
scd.partial = as_scd.msg(msg.partial)
scd.update = as_scd.msg(msg.update)
scd.delete = as_scd.msg(msg.delete)
scd.insert = as_scd.msg(msg.insert)


# Insert orderBookL2
test_that('insert_orderBookL2', {
  init_orderBookL2(conn)
  insert_orderBookL2(conn, scd.partial)
  expect_equal(tbl(conn, 'orderBookL2') %>% count() %>% pull() %>% as.numeric(), 9218)
  expect_true(tbl(conn, 'orderBookL2') %>% filter(id=='8799284700') %>% pull() %>% is.na())
  expect_true(tbl(conn, 'orderBookL2') %>% filter(id=='8799284650') %>% pull() %>% is.na())
  dbGetQuery(conn, 'select * from "orderBookL2"
             where end_time is null and symbol=$1 and id=$2 and side=$3',
             params=list('XBTUSD', '8799284700', 'Buy'))
  dbGetQuery(conn, 'select * from "orderBookL2"
             where end_time is null and symbol=$1 and id=$2 and side=$3',
             params=list('XBTUSD', '8799284650', 'Buy'))
  dbGetQuery(conn, 'select * from "orderBookL2"
             where end_time is null and symbol=$1 and id in ($2) and side=$3',
             params=list('XBTUSD', '8799284650', 'Buy'))
})

# Update orderBookL2
test_that('update_orderBookL2', {
  init_orderBookL2(conn)
  insert_orderBookL2(conn, scd.partial)
  expect_equal(update_orderBookL2(conn, scd.update), list(1, 1))
  expect_false(tbl(conn, 'orderBookL2') %>% filter(id=='8799284700') %>% pull() %>% is.na())
  expect_false(tbl(conn, 'orderBookL2') %>% filter(id=='8799284650') %>% pull() %>% is.na())
})

test_that('import_orderBookL2', {
  init_orderBookL2(conn)
  filenames = c('data-raw/orderBookL2.txt', 'data-raw/orderBookL2_2.txt')
  import_orderBookL2(conn, filenames, info=TRUE)
})
