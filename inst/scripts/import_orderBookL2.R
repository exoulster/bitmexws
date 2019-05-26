library(tidyverse)
library(mongolite)
library(parallel)
library(RPostgres)
library(dbplyr)


# filenames = dir('/Volumes/Storage/bitmex_data/orderBookL2/2019-05-22_06/', full.names = TRUE)
filenames = dir('~/Downloads/orderBookL2', pattern='orderBookL2\\.txt.+', full.names=TRUE, recursive=TRUE)
filenames


conn = dbConnect(RPostgres::Postgres(), dbname='bitmex')
init_orderBookL2(conn)
system.time(import_orderBookL2(conn, filenames[1:3]))

tbl(conn, 'orderBookL2_minute')
tbl(conn, 'meta_orderBookL2')
