#
#         size  price
# insert    Y     Y
# upadte    Y     N
# delete    N     N
#
# fill size of delete to 0
#



#' @import dplyr
#' @export
read_jsonlines = function(filename) {
  ob.lines = readr::read_lines(filename)
  lapply(ob.lines, function(row) {
    jsonlite::fromJSON(row, simplifyVector=FALSE)
  })
}


#' @import dplyr
#' @export
to_jsonlines = function(ob) {
  as.list() %>% unname() %>% jsonlite::toJSON(auto_unbox = TRUE)
}


#' Read msgs from file
#' @import dplyr
#' @export
read_msgs = function(filename) {
  ob.lines = readr::read_lines(filename)
  lapply(ob.lines, function(row) {
    msg = jsonlite::fromJSON(row, simplifyVector=FALSE)
    data = as_orderBookL2.list(msg$data)
    msg$data = data
    msg
  }) %>% as_msgs()
}


#' Convert plain jsonlines to msgs object
#' @param msgs msgs object
#' @description msgs object is list of websocket message
#' @export
as_msgs = function(msgs) {
  if (!inherits(msgs, 'msgs')) {
    class(msgs) = c('msgs', class(msgs))
  }
  msgs
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
as_orderBookL2.list = function(x) {
  if (inherits(x, 'orderBookL2')) return(x)
  keys = c('symbol', 'id', 'side')
  names(x) = sapply(x, function(order) paste(order[keys], collapse='_'))
  x = as.environment(x)
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


# list_update = function(l1, l2) {
#   e1 = as.environment(l1)
#   e2 = as.environment(l2)
#   lapply(names(e2), function(nm) {
#     e1[[nm]] = e2[[nm]]
#   })
#   lst = as.list(e1)
#   # lst = lst[c('symbol', 'id', 'side', 'size', 'price')]
#   lst = lst[!sapply(lst, is.null)]
#   return(lst)
# }


#' @export
copy = function(x, ...) {
  UseMethod('copy')
}


#' @export
copy.orderBookL2 = function(ob) {
  e = as.list(ob) %>%
    as.environment()
  if (!inherits(e, 'orderBookL2')) {
    class(e) = c('orderBookL2', class(e))
  }
  return(e)
}
# sapply(names(new_ob), function(nm) { identical(ob[[nm]], new_ob[[nm]]) }) %>% all()


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
  }, msgs, obs[2:length(obs)])
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