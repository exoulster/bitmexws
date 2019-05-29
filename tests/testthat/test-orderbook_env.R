context("test-orderbook_env")

# skip('skipping orderbook_env')

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
scd.origin = as_scd.msg(msgs[[1]])
scd.increment = as_scd.msg(msgs[[2]])


test_that('as_scd', {
  expect_true(as_scd(msg.update) %>% pull(end_time) %>% is.na() %>% all())
  expect_equal(as_scd(msg.update) %>% nrow(), 2)
  expect_true(as_scd(msg.update) %>% inherits('scd'))
  expect_true(as_scd.orderBookL2(inc.update, action='update', timestamp='20190101') %>% pull() %>% is.na() %>% all())
  expect_equal(as_scd.orderBookL2(inc.update, action='update', timestamp='20190101') %>% nrow(), 2)
  expect_true(as_scd.orderBookL2(inc.update, action='update', timestamp='20190101') %>% inherits('scd'))
})

test_that('merge', {
  res = merge(as_scd(msg.partial), as_scd(msg.update))
  expect_equal(names(res) = c('to_update', 'to_insert'))
  expect_true(res$to_insert %>% pull() %>% is.na() %>% all())
})

test_that('new_orderBookL2', {
  ob = new_orderBookL2()
  expect_true(inherits(ob, 'environment'))
  expect_true(inherits(ob, 'orderBookL2'))
})

test_that('as_orderBookL2.list', {
  ob2 = as_orderBookL2.list(inc.update)
  expect_equal(length(ob2), 2)
  expect_true('XBTUSD_8799284650_Buy' %in% names(ob2))
  expect_true('XBTUSD_8799284700_Buy' %in% names(ob2))
})

test_that('as_tibble.orderBookL2', {
  ob2 = as_orderBookL2.list(inc.update)
  expect_equal(as_tibble(ob2) %>% nrow(), 2)
})

test_that("modify.orderBookL2", {
  # testing the update logic
  ob = as_orderBookL2.list(inc.partial)
  l1 = ob[['XBTUSD_8799284650_Buy']]
  l2 = inc.update[['XBTUSD_8799284650_Buy']]
  modify.orderBookL2(ob, inc.update, action='update')
  expect_identical(ob[['XBTUSD_8799284650_Buy']], purrr::list_modify(l1, !!!l2))

  # testing the partial logic: partial -> update -> partial = partial
  ob = as_orderBookL2.list(inc.partial)
  ob2 = as_orderBookL2.list(inc.partial)
  modify.orderBookL2(ob2, inc.update, action='update')
  modify.orderBookL2(ob2, inc.partial, action='partial')  # update by reference
  expect_identical(ob[['XBTUSD_8799284650_Buy']], ob2[['XBTUSD_8799284650_Buy']])
  expect_identical(ob[['XBTUSD_8799284700_Buy']], ob2[['XBTUSD_8799284700_Buy']])

  # testing the delete logic
  ob = as_orderBookL2.list(inc.partial)
  ob2 = as_orderBookL2.list(inc.delete)
  modify.orderBookL2(ob, inc.delete, action='delete')
  expect_identical(ob[['XBTUSD_8799318350_Buy']][['size']], 0)

  # testing if it works with empty orderbook
  ob = new_orderBookL2()
  ob2 = as_orderBookL2.list(inc.update)
  modify.orderBookL2(ob, inc.update, action='update')  # update by reference
  expect_identical(ob[['XBTUSD_8799284650_Buy']], ob2[['XBTUSD_8799284650_Buy']])
  expect_identical(ob[['XBTUSD_8799284700_Buy']], ob2[['XBTUSD_8799284700_Buy']])
})

test_that('reduce_msgs', {
  msgs = read_msgs(filename)
  msg = reduce_msgs(msgs)
  expect_equal(length(msg$data), 9235)
  msgs = read_msgs(filename)
  msg = reduce_msgs(msgs, strict=TRUE)
  expect_equal(length(msg$data), 9218)
})

test_that('accumulate_msgs', {
  msgs = read_msgs(filename)
  new_msgs = accumulate_msgs(msgs[1:100])
  expect_equal(length(new_msgs[[100]]$data), 9219)
})
