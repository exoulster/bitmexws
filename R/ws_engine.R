# 1. event$data ->
# 2. parse_msg_raw(event$data) -> msg
# 3. parse_msg_data(msg) -> list of records
# 4. make_snapshot(records) -> snapshot
# 5. save()


#' Parse raw message from event$data
#' @export
parse_msg_raw = function(event_data) {
  msg = jsonlite::fromJSON(event_data)
  if (msg[[1]] %in% c(0, 1)) {  # multiplexing result
    msg = msg[[4]]
  }
  msg$localtime = Sys.time()
  msg
}


#' Parse msg$data, return data.frame
#' @export
parse_msg_data = function(msg) {
  if (class(msg)=='character') {  # char, try parse
    msg = parse_msg_raw(msg)
  }
  data = msg$data
  data$action = msg$action
  data$servertime = msg$timestamp
  data$localtime = msg$localtime
  tibble::as.tibble(data)
}


# ob.10000.sim.nest = simplify_ob_events(ob.10000) %>%
#   mutate(ts=localtime) %>%
#   group_by(ts) %>%
#   nest()
# > system.time(a <- reduce(ob.10000.nest$data, ob_increment))
# user  system elapsed
# 11.432   3.943  15.382
# > system.time(b <- reduce(ob.10000.sim.nest$data, ob_increment))
# user  system elapsed
# 1.929   0.686   2.619
# > identical(a, b)
# [1] TRUE
simplify_ob_events = function(ob_events) {
  # keep all partial
  # keep last update
  # keep all insert, delete
  # p -> d -> i
  # p -> d -> i -> d
  # p -> d
  idx.update = which(ob_events$action=='update')
  updates = ob_events[idx.update, ] %>%
    dplyr::group_by(id, side) %>%
    dplyr::summarise_all(last)
  ob_events[-idx.update, ] %>%
    dplyr::bind_rows(updates) %>%
    dplyr::arrange(localtime)
}

# system.time(c <- ob_snapshot(ob.10000))
ob_snapshot = function(ob_events, key=list('id', 'side')) {
  # group by id, side
  # if partial -> keep
  # if delete -> remove
  # if insert -> keep
  ob_events %>%
    dplyr::arrange(localtime) %>%
    dplyr::group_by_(.dots=key) %>%
    dplyr::summarise(
      symbol=last(symbol),
      size=last(size),
      price=first(price),
      action=last(action),
      localtime=last(localtime)
    ) %>%
    dplyr::filter(action!='delete') %>%  # remove delete records
    dplyr::filter(!is.na(price)) %>%  # remove missing prices
    dplyr::select(symbol, id, side, size, price, action, localtime) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(desc(price))
}


ob_snapshot_each = function(ob_events) {
  ob_events %>%
    mutate(ts = localtime) %>%
    group_by(ts) %>%
    nest() %>%
    summarise(events=rolling_back())
}


#' Make snapshot from event records
#' Returns tibble (hash table)
#' @export
ob_increment = function(snapshot, increment, key=c('id', 'side')) {
  if (is.null(snapshot)) {
    return(increment)
  }

  # partial, update, insert, delete
  idx.update = increment$action=='update'
  idx.insert = increment$action=='insert'
  idx.delete = increment$action=='delete'

  # modified_update = snapshot %>%  # use new values from increment, unless not found
  #   dplyr::inner_join(increment[idx.update, ], by=key, suffix=c('.x', '')) %>%
  #   select(-tidyselect::ends_with('.x'))

  snapshot = snapshot %>%
    # ob_delete(increment[idx.delete, ]) %>%
    ob_update(increment[idx.update, ]) %>%
  # snapshot = snapshot %>%
  #   dplyr::anti_join(increment[idx.update, ], by=key) %>%  # remove rows being updated
    dplyr::anti_join(increment[idx.delete, ], by=key) %>%  # remove delete
    # dplyr::bind_rows(modified_update) %>%  # append update
    dplyr::bind_rows(increment[idx.insert, ])  # append insert
    # dplyr::arrange(desc(price))
  snapshot
}

ob_delete = function(snapshot, data_dfr) {
  if (nrow(data_dfr)==0) {
    return(snapshot)
  }

  idxs = sapply(seq(1, nrow(data_dfr)), function(rn) {
    row = data_dfr[rn, ]
    idx = which(snapshot$id==row$id & snapshot$side==row$side)
    if (length(idx)>0) {
      idx
    }
  }, USE.NAMES = FALSE)
  print(idxs)
  snapshot[-idxs,]
}


ob_update = function(snapshot, data_dfr) {
  if (nrow(data_dfr)==0) {
    return(snapshot)
  }

  lapply(seq(1, nrow(data_dfr)), function(rn) {
    row = data_dfr[rn, ]
    idx = which(snapshot$id==row$id & snapshot$side==row$side)
    if (length(idx)>0) {
      snapshot[idx, ]$size <<- row$size
      snapshot[idx, ]$action <<- row$action
      snapshot[idx, ]$localtime <<- row$localtime
    }
  })
  snapshot
}


best_price = function(snapshot, top=1, output='long') {
  orders = snapshot %>%
    arrange(desc(price)) %>%
    split(.$side)
  b = head(orders$Buy, top)
  s = tail(orders$Sell, top)
  if (output=='long') {
    dplyr::bind_rows(s, b)
  } else if (output=='wide') {
    m = matrix(c(b$size, b$price, s$price, s$size), ncol=4)
    dfr = data.frame(m)
    names(dfr) = c('bidSize', 'bidPrice', 'askPrice', 'askSize')
    tibble::as.tibble(dfr)
  }
}


#' Save data in the form of data.frame
#' @export
save_data = function(data, fname, append=TRUE) {
  # if (!is.list(records)) stop('records is not a list')
  if (!is.data.frame(data)) stop()
  data = mutate_if(data, is.numeric, as.character)  # convert number to character to avoid scientific notation
  readr::write_tsv(data, fname, append=append)
  TRUE
}

# as.ob_events = function(records) {
#   events = bind_rows(records)
#   events$action = as.factor(events$action)
# }
