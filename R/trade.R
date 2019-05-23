
#' @import dplyr
#' @export
read_trade = function(filename) {
  trade.lines = readr::read_lines(filename)
  if (length(trade.lines) == 0) return(tibble::tibble())
  lapply(trade.lines, function(row) {
    jsonlite::fromJSON(row)$data
  }) %>%
    bind_rows() %>%
    tibble::as_tibble() %>%
    mutate(timestamp = readr::parse_datetime(timestamp))
}


#' @export
to_kline = function(trade, unit='minute') {
  kline = trade %>%
    arrange(timestamp) %>%
    mutate(dt = lubridate::floor_date(timestamp, unit=unit)) %>%
    group_by(dt) %>%
    summarise(open = first(price),
              high = max(price),
              low = min(price),
              close = last(price),
              price_range = close - open,
              price_range2 = sign(close-open) * abs(high-low),
              volume = sum(size)/1000000,
              cnt = n(),
              order_flow = sum(ifelse(side=='Buy', size, -1 * size)),
              return = close / open,
              max_return = high / open,
              min_return = low / open,
              direction = ifelse(close>=open, 'up', 'down'),
              MinusTick = sum(tickDirection=='MinusTick'),
              PlusTick = sum(tickDirection=='PlusTick'),
              ZeroMinusTick = sum(tickDirection=='ZeroMinusTick'),
              ZeroPlusTick = sum(tickDirection=='ZeroPlusTick')) %>%
    mutate(
      roc = lead(return - 1),  # roc is the rate of change (incremental)
      max_roc = lead(max_return - 1),
      min_roc = lead(min_return - 1),
      return = lead(return),  # return is actually the next-day return
      max_return = lead(max_return),
      min_return = lead(min_return),
      long_at_open_price = low > open,  # could open position at open price, if price goes lower later
      close_long_at_close_price = lead(high) > close) %>%
    filter(!is.na(roc))
  class(kline) = c('kline', class(kline))
  return(kline)
}


#' @import dplyr
price_move = function(x, percent=FALSE) {
  high = max(x)
  low = min(x)
  open = first(x)
  close = last(x)

  pm = close - open
  pm_pct = close / open - 1
  if (percent) {
    return(pm_pct)
  } else {
    return(pm)
  }
}
