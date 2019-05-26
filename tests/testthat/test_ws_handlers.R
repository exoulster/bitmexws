context('Testing BitMEX websocket handlers')

event_instrument = list(data = msg.instrument.partial)
event_orderBookL2_partial = list(data = msg.orderBookL2.partial)
event_orderBookL2_update = list(data = msg.orderBookL2.update)
e = new.env()

test_that('fname_split', {
  expect_equal(fname_split('trade:XBTUSD'), c('trade-XBTUSD', 'txt'))
  expect_equal(fname_split('trade:XBTUSD', default_ext='csv'), c('trade-XBTUSD', 'csv'))
  expect_equal(fname_split('trade:XBTUSD.log'), c('trade-XBTUSD', 'log'))
  expect_equal(fname_split('trade:XBTUSD.log', default_ext='csv'), c('trade-XBTUSD', 'log'))
})

test_that('fname_attach_ts', {
  ts = Sys.time()
  dt = strftime(ts, format='%Y%m%dT000000%z')
  expect_equal(fname_attach_ts('abc'), paste('abc_', dt, '.txt', sep=''))
  expect_equal(fname_attach_ts('abc', default_ext='log'), paste('abc_', dt, '.log', sep=''))
})

test_that('Log Handler', {
  lh = log_handler(env=e, fname='log_test.txt', attach_ts=FALSE, chunk_size=2)
  lh(event_orderBookL2_partial)
  expect_equal(length(e$tmp_log), 1)
  lh(event_orderBookL2_update)
  expect_equal(length(e$tmp_log), 0)
})

test_that('Stream Handler', {
  sh = stream_handler(env=e, buffer=10)
  expect_false(e$ACTIVE_STREAM)  # FALSE before 'partial' arrives
  sh(event_orderBookL2_partial)
  expect_true(e$ACTIVE_STREAM)  # TRUE after 'partial' arrives
  expect_false(is.null(e$data))
  expect_equal(nrow(e$data), 10)
  expect_equal(names(e$data), c('symbol', 'id', 'side', 'size', 'price', 'action', 'localtime'))
})

test_that('File Handler', {
  fh = file_handler(env=e, fname='orderBookL2_XBTUSD', chunk_size=10000)
  fh(event_orderBookL2_partial)
  expect_equal(nrow(e$tmp_data), 6660)  # 6660 rows
  expect_equal(names(e$tmp_data), c('symbol', 'id', 'side', 'size', 'price', 'action', 'localtime'))

  fh = file_handler(env=e, fname='orderBookL2_XBTUSD', chunk_size=5)
  expect_false(e$ACTIVE_FILE)  # FALSE before 'partial' arrives
  fh(event_orderBookL2_partial)  # process partial data
  expect_true(e$ACTIVE_FILE)  # TRUE after 'partial' arrives
  expect_equal(nrow(e$tmp_data), 0)  # should be 0 rows after saving

  fh(event_orderBookL2_update)  # process update data
  expect_equal(nrow(e$tmp_data), 0)
  expect_equal(names(e$tmp_data), c('symbol', 'id', 'side', 'size', 'price', 'action', 'localtime'))
  expect_true(all(is.na(e$tmp_data$price)))
})

test_that('Snapshot Handler', {
  sph = snapshot_handler(env=e, key=c('id', 'side'))
  expect_false(e$ACTIVE_SNAPSHOT)
  sph(event_orderBookL2_partial)
  expect_true(e$ACTIVE_SNAPSHOT)
  expect_equal(nrow(e$snapshot), 6660)

  sph(event_orderBookL2_update)
  expect_equal(nrow(e$snapshot), 6660)
  expect_equal(sum(e$snapshot$action=='update'), 5)
})

# cleanups
system('rm *.txt')
