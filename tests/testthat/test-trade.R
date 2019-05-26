context("test-trade")

skip('skipping trade')

setwd('../../')
trade.lines = read_lines('data-raw/trade.txt')
trade.lines[2]

read_trade('data-raw/trade.txt')


test_that("multiplication works", {
  expect_equal(2 * 2, 4)
})
