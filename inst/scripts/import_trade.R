library(tidyverse)
library(bitmexws)
library(RPostgres)
library(lubridate)
library(dbplyr)


conn = dbConnect(RPostgres::Postgres(), dbname='bitmex')
conn = dbConnect(RPostgres::Postgres(), dbname='bitmex',
                 host='localhost', port=5433, user='exoulster')


# Initialisation
init_trade(conn)

# Import
filenames = dir('~/Downloads/trade', pattern='trade\\.txt.+', full.names=TRUE, recursive=TRUE)
filenames
import_trade(filenames)


# Verify
tbl(conn, 'trade') %>%
  count()
