#
#         size  price
# insert    Y     Y
# upadte    Y     N
# delete    N     N
#
# fill size of delete to 0
#

id2price = function(id, symbol='XBTUSD') {
  if (symbol=='XBTUSD') symbolIdx = 88
  return(((100000000 * symbolIdx) - id) / 100)
}


#' @import dplyr
#' @export
read_jsonlines = function(filename) {
  ob.lines = readr::read_lines(filename)
  lapply(ob.lines, function(row) {
    # jsonlite::fromJSON(row, simplifyVector=FALSE)
    rjson::fromJSON(row)
  })
}


#' Read msgs from file
#' @import dplyr
#' @export
read_msgs = function(filename) {
  ob.lines = readr::read_lines(filename)
  lapply(ob.lines, function(row) {
    msg = rjson::fromJSON(row)
    msg$data = as_orderBookL2.list(msg$data)
    msg = as_msg(msg)
    msg
  })
}


#' @import dplyr
#' @export
to_json = function(ob) {
  ob %>% as.list() %>% unname() %>% rjson::toJSON()
}

#' @export
as_json = function(x, ...) {
  UseMethod('as_json')
}

#' @import dplyr
#' @export
as_json.orderBookL2 = function(ob) {
  to_json(ob)
}


#' #' @import dplyr
#' #' @export
#' to_scd = function(ob, timestamp=NULL) {
#'   scd = ob %>%
#'     as_tibble() %>%
#'     mutate(key=paste(symbol, id, side, sep='_'))
#'   if(!is.null(timestamp))
#'     scd$start_date = timestamp
#'   else
#'     scd$start_date = lubridate::now()
#'   scd$end_date = NA
#'   return(scd)
#' }


#' Convert plain json object to msg object
#' @param msg msg object
#' @description msg object is list of websocket message
#' @export
as_msg = function(msg) {
  if (!inherits(msg, 'msg')) {
    class(msg) = c('msg', class(msg))
  }
  msg
}

#' @export
print.msg = function(x) {
  cat(paste("BitMEX Websocket Message of", x[['table']]))
  cat('\n')
  cat(paste('Action:', x[['action']]))
  cat('\n')
  cat(paste('Timestamp:', x[['timestamp']]))
  cat('\n')
  print(x['data'])
}

#' @export
as_scd = function(msg) {
  UseMethod('as_scd')
}

#' @import tidyr dplyr
#' @export
as_scd.msg = function(msg) {
  scd = as_tibble(msg$data) %>%
    mutate(
      action = msg[['action']],
      start_time = msg[['timestamp']],
      end_time = NA,
    )
  if (!inherits(scd, 'scd'))
    class(scd) = c('scd', class(scd))
  return(scd)
}

#' @import tidyr dplyr
#' @export
as_scd.orderBookL2 = function(ob, action, timestamp) {
  scd = as_tibble(ob) %>%
    mutate(
      action = action,
      start_time = timestamp,
      end_time = NA
    )
  if (!inherits(scd, 'scd'))
    class(scd) = c('scd', class(scd))
  return(scd)
}

#' @import tidyr dplyr
#' @export
as_scd.data.frame = function(scd) {
  if (!inherits(scd, 'scd'))
    class(scd) = c('scd', class(scd))
  return(scd)
}


#' @export
merge_scd = function(x, y) {
  UseMethod('merge_scd')
}

#' @import tidyr dplyr
#' @export
merge_scd.scd = function(scd.origin, scd.increment) {
  if (nrow(scd.origin)==0) {
    return(list(
      to_update = scd.origin,
      to_insert = scd.increment
    ))
  }
  keys = syms(c('symbol', 'id', 'side'))
  scd.origin = scd.origin %>% mutate(tag='origin')
  scd.increment = scd.increment %>% mutate(tag='increment')
  dfr = scd.origin %>%
    bind_rows(scd.increment) %>%
    group_by(!!!keys) %>%
    mutate(end_time = lead(start_time)) %>%
    ungroup()
  scd.update = dfr %>%
    filter(tag=='origin', !is.na(end_time)) %>%
    select(-tag)
  scd.insert = dfr %>%
    filter(tag == 'increment') %>%
    select(-tag)
  list(
    to_update = scd.update,
    to_insert = scd.insert
  )
}

#' @import tidyr dplyr
#' @export
merge_scd.list = function(msgs) {
  keys = syms(c('symbol', 'id', 'side'))
  lapply(msgs, as_scd) %>%
    bind_rows() %>%
    group_by(!!!keys) %>%
    mutate(
      end_time = lead(start_time)
    ) %>%
    ungroup() %>%
    mutate(
      id = as.character(id)
    )
}


#' @export
new_orderBookL2 = function() {
  ob = new.env()
  class(ob) = c('orderBookL2', class(ob))
  return(ob)
}


#' @export
as_orderBookL2 = function(x, ...) {
  UseMethod('as_orderBookL2')
}


#' @export
as_orderBookL2.list = function(x, na.fill=TRUE) {
  if (inherits(x, 'orderBookL2')) return(x)
  if (na.fill)
    x = lapply(x, function(order) {
      if (is.null(order[['price']]))
        order[['price']] = id2price(order[['id']])
      if (is.null(order[['size']]))
        order[['size']] = 0
      order
    })
  keys = c('symbol', 'id', 'side')
  names(x) = sapply(x, function(order) paste(order[keys], collapse='_'))
  x = as.environment(x)
  class(x) = c('orderBookL2', class(x))
  x
}

#' @export
as_orderBookL2.environment = function(x) {
  if (inherits(x, 'orderBookL2')) return(x)
  class(x) = c('orderBookL2', class(x))
  x
}

#' @export
as_orderBookL2.default = as_orderBookL2.list


#' @export
print.orderBookL2 = function(x) {
  x.tbl = as_tibble(x)
  print(paste(length(x), 'orders in orderBookL2'))
}


#' @export
as_tibble.orderBookL2 = function(ob) {
  do.call(bind_rows, as.list(ob))
}

#' @export
as.list.orderBookL2 = function(ob) {
  ob %>%
    as.list.environment() %>%
    unname()
}


#' @export
copy = function(x, ...) {
  UseMethod('copy')
}


#' @import dplyr
#' @export
copy.orderBookL2 = function(ob) {
  as.list.orderBookL2(ob) %>%
    as_orderBookL2()
}


#' @export
modify = function(x, ...) {
  UseMethod('modify')
}

#' Update orderBookL2
#' @details
#' modify by reference!!
modify.orderBookL2 = function(ob, inc, action='update', strict=FALSE, copy=FALSE) {
  keys = c('symbol', 'id', 'side')

  if (copy) {
    ob = copy.orderBookL2(ob)
  }

  if (strict) {
    invisible(lapply(inc, function(order) {
      key = paste(order[keys], collapse='_')
      if (action=='delete') {
        if (!is.null(ob[[key]])) ob[[key]] = NULL
      } else if (action=='partial') {
        ob[[key]] = order
      } else {
        if (!is.null(ob[[key]])) ob[[key]] = modifyList(ob[[key]], order) # modify if not NULL
      }
    }))
  } else {
    if (action=='partial') {
      ob = new_orderBookL2()
    }
    invisible(lapply(inc, function(order) {
      key = paste(order[keys], collapse='_')
      if (action=='delete') order[['size']] = 0
      if (is.null(ob[[key]])) ob[[key]] = list()  # fill NULL with empty list
      ob[[key]] = modifyList(ob[[key]], order)
    }))
  }

  if (!inherits(ob, 'orderBookL2')) {
    class(ob) = c('orderBookL2', class(ob))
  }
  return(ob)
}

#
# merge_tibble = function(ob, inc, action='update') {
#
# }


#' @import dplyr
#' @export
reduce_msgs = function(msgs, limit=0, strict=FALSE) {
  if (limit==0) limit = length(msgs)
  msgs = msgs[1:limit]

  data = lapply(msgs, function(msg) msg$data)
  action = lapply(msgs, function(msg) msg$action)
  f = purrr::partial(modify.orderBookL2, strict=strict)
  ob = purrr::reduce2(data, action, f, .init=new_orderBookL2())

  new_msg = msgs[[length(msgs)]]  # the last msg
  new_msg$data = ob
  if (strict) new_msg$action = 'partial' else new_msg$action = 'update'
  return(new_msg)
}


#' @import dplyr
#' @export
accumulate_msgs = function(msgs, limit=0, strict=FALSE) {
  if (limit==0) limit = length(msgs)
  msgs = msgs[1:limit]

  data = lapply(msgs, function(msg) msg$data)
  action = lapply(msgs, function(msg) msg$action)
  f = purrr::partial(modify.orderBookL2, strict=strict, copy=TRUE)
  obs = purrr::accumulate2(data, action, f, .init=new_orderBookL2())

  mapply(function(msg, ob) {
    new_msg = msg
    new_msg$data = ob
    new_msg$action = 'partial'
    return(new_msg)
  }, msgs, obs[2:length(obs)], SIMPLIFY=FALSE)
}


#' @import dplyr
#' @export
map_msgs = function(msgs, unit='seconds') {
  idx = lapply(msgs, function(row) {
    tibble::tibble(
      timestamp = row$timestamp,
      action = row$action,
      table = row$table
    )
  }) %>%
    bind_rows() %>%
    mutate(timestamp = readr::parse_datetime(timestamp)) %>%
    mutate(dt = lubridate::floor_date(timestamp, unit=unit)) %>%
    group_by(dt)

  split(msgs, group_indices(idx))
}
