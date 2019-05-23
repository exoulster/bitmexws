library(websocket)
library(testthat)

# Field Names: https://www.onixs.biz/fix-dictionary/5.0.SP2/msgType_AE_6569.html

# uid not found in snapshot


register_handler = function(ws, handler) {
  ws$onMessage(callback=handler)
}

#' @param args e.g. orderBookL2:XBTUSD
#' @export
subscribe = function(args, progress=FALSE, stream=FALSE, file=FALSE, log=FALSE, path=NULL, compress=TRUE, snapshot=FALSE, multiplexing=TRUE, max_char=100, ttl=Inf) {
  options(digits.secs=6)  # for greater timestamp accuracy

  if (multiplexing) {
    url = 'wss://www.bitmex.com/realtimemd'
    opening_cmd = jsonlite::toJSON(list(1, "abc", "user"), auto_unbox=TRUE)
    # closing_cmd = jsonlite::toJSON(list(2, "abc", "user"), auto_unbox=TRUE)
    cmd = list(op="subscribe", args=args)
    subscribing_cmd = jsonlite::toJSON(list(0, "abc", "user", cmd), auto_unbox = TRUE)
  }
  else {
    url = 'wss://www.bitmex.com/realtime'
    subscribing_cmd = jsonlite::toJSON(list(op="subscribe", args=args), auto_unbox = TRUE)
  }
  api_key = Sys.getenv('BITMEX_API_KEY')
  api_secret = Sys.getenv('BITMEX_API_SECRET')
  args_parts = strsplit(args, ':')[[1]]
  key = switch(
    EXPR = args_parts[1],
    'orderBookL2' = c('id', 'side'),
    'instrument' = 'symbol'
  )

  # Subscribe to feed, initialize
  e = new.env()
  e$ws = websocket::WebSocket$new(url=url, autoConnect = TRUE)
  e$ws$onOpen(callback=send_handler(cmd=opening_cmd))
  e$ws$onMessage(callback=subscribe_handler(cmd=subscribing_cmd))
  e$ws$onMessage(callback=init_handler(env=e))

  # Register handlers
  if (progress) {
    e[['progress_handler']] = e$ws$onMessage(callback=progress_handler(max_char=max_char, level='info'))
  }
  if (stream) {
    e[['stream_handler']] = e$ws$onMessage(callback=stream_handler(env=e, buffer=7000))
  }
  if (file) {
    e[['file_handler']] = e$ws$onMessage(callback=file_handler(env=e, fname=paste(path, paste(args_parts, collapse='_'), sep='')))
  }
  if (log) {
    e[['log_handler']] = e$ws$onMessage(callback=log_handler(env=e, fname=paste(path, paste(args_parts, collapse='_'), sep='')))
  }
  if (snapshot) {
    e[['snapshot_handler']] = e$ws$onMessage(callback=snapshot_handler(env=e, key=key))
  }

  if (!is.infinite(ttl)) e$ws$onMessage(callback=countdown_handler(env=e, ttl=ttl))

  if (compress) e$ws$onClose(callback=zip_handler(env=e))

  return(e)
}


feed_recorder = function(feed, progress=TRUE, path=NULL, compress=TRUE, rollover=TRUE) {
  e = subscribe(args=feed, progress=progress, file=TRUE, path=path, compress=compress)
  args_parts = strsplit(feed, ':')[[1]]
  tryCatch(
    while (e$ws$readyState() <= 1L) {
      later::run_now(0.01)
      if (rollover) {
        cur_dt = lubridate::floor_date(Sys.time(), unit='min')
        last_dt = lubridate::floor_date(e$LAST_TS_FILE, unit='min')
        if (cur_dt > last_dt) {  # crossing date
          print('switch over')
          e$file_handler()  # deregister previous handler
          print('deregistered previous handler')
          e[['file_handler']] = e$ws$onMessage(callback=file_handler(env=e, attach_ts=FALSE, fname=paste(path, paste(args_parts, collapse='_'), sep='')))
        }
      }
    },
    interrupt = function(cond) {
      e$ws$close()
    }
  )
}
