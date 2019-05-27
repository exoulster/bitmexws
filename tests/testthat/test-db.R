context("test-db")

library(RPostgres)
conn = dbConnect(RPostgres::Postgres(), dbname='bitmex')

# Initialisation
init_trade(conn)

# Import
filenames = dir('~/Downloads/trade', pattern='trade\\.txt.+', full.names=TRUE, recursive=TRUE)
filenames
import_trade(conn, filenames, full=TRUE)

test_that("multiplication works", {
  expect_equal(2 * 2, 4)
})
