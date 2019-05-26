context("test-orderbook")

skip('skipping orderbook')

library(readr)
library(jsonlite)
library(tibble)

setwd('../../')
ob.tbl = read_orderbookl2('data-raw/orderBookL2.txt')
keys = get_keys(ob.lines[1])
ob = init_ob(ob.lines[1])
# accumulate(ob.lines[2:5], parse_ob, .init=ob)

origin = ob.tbl[1,]
increment = ob.tbl[2,]

frame.partial = fromJSON(ob.lines[1])[['data']] %>% as_tibble()
frame.update = fromJSON(ob.lines[2])[['data']] %>% as_tibble()
frame.delete = fromJSON(ob.lines[38])[['data']] %>% as_tibble()
frame.insert = fromJSON(ob.lines[74])[['data']] %>% as_tibble()



test_that("orderbook basic operation", {
  expect_identical(insert(ob, frame.insert) %>% anti_join(ob), frame.insert)
  expect_identical(update(ob, frame.update) %>% inner_join(frame.update) %>% select(-price), frame.update)
  expect_identical(delete(ob, frame.delete) %>% anti_join(ob) %>% select(-size, -price), frame.delete)
  expect_equal(delete(ob, frame.delete) %>% anti_join(ob) %>% .$size, 0) # size = 0

  # test empty tibble
  expect_identical(insert(tibble(), frame.insert), frame.insert)
  expect_identical(update(tibble(), frame.update), frame.update)
  expect_identical(delete(tibble(), frame.delete) %>% select(-size), frame.delete)
})


test_that('update_orderbook', {
  expect_identical(update_orderbook(origin, increment)$data[[1]], update(frame.partial, frame.update))

  n = 100
  A = reduce(split(ob.tbl[1:n,], 1:n), update_orderbook)
  B.1 = ob.tbl[1,]
  B.2 = reduce(split(ob.tbl[2:n,], 1:(n-1)), update_orderbook)
  B = update_orderbook(B.1, B.2)
  expect_identical(A, B)
})
