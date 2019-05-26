context('Testing BitMEX websocket client functionalities')

msg = parse_msg_raw(msg.orderBookL2.partial)
# data = parse_msg_data(msg.orderBookL2.partial)
ob_partial = parse_msg_data(msg.orderBookL2.partial)  # nrow=6660
ob_update = parse_msg_data(msg.orderBookL2.update)  # nrow=6
ob_events = bind_rows(ob_partial, ob_update)

test_that('Test parse_msg_raw', {
  expect_equal(length(parse_msg_raw(msg.instrument.update)), 4)
  expect_equal(parse_msg_raw(msg.instrument.update)$table, 'instrument')
  expect_equal(class(parse_msg_raw(msg.subscribe.multiplexing)), 'list')  # test multiplexing results
})

test_that('Test parse_msg_data', {
  expect_true(inherits(ob_events, 'data.frame'))  # class is data.frame
  expect_equal(nrow(ob_events), 6666)  # equal number of rows
  expect_equal(unique(parse_msg_data(msg.orderBookL2.partial)$action), 'partial')  # action is partial
  expect_equal(unique(parse_msg_data(msg.orderBookL2.update)$action), 'update')
})

test_that('Test ob_increment', {
  sp = ob_increment(ob_partial, ob_update, key=c('id', 'side'))
  expect_equal(nrow(sp), 6660)
  expect_equal(sum(sp$action=='update'), 5)  # 6 updates?
})

test_that('Test ob_snapshot', {
  sp = ob_snapshot(ob.10000)
  expect_equal(nrow(sp), 6649)
  expect_equal(sum(sp$action=='partial'), 6114)
  expect_equal(sum(sp$action=='update'), 534)
  expect_equal(sum(sp$action=='delete'), 0)
  expect_equal(sum(sp$action=='insert'), 1)
})

test_that('Test save_data', {
  expect_true(save_data(ob_events, '/tmp/orderBookL2.txt', append=FALSE))
  system('rm /tmp/orderBookL2.txt')
})
