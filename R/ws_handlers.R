##########################################
# Handlers
##########################################
# # pong = ws$onMessage(callback=function(event) {
# #   if (event$data == 'ping') {
# #     ws$send('pong')
# #   }
# # })

# init_dfr = function(col_names) {
#   dfr = data.frame(matrix(ncol=length(col_names), nrow=0))
#   names(dfr) = col_names
#   tibble::as.tibble(dfr)
# }


fname_split = function(fname, default_ext='txt', replace='[:;]', ...) {
  parts = strsplit(fname, '.', fixed=TRUE)[[1]]
  if (length(parts)==1) parts[2] = default_ext
  parts[1] = sub(replace, '-', parts[1])  # substitute ';' by '-'
  l = length(parts)
  main = paste(parts[1:l-1], collapse='.')
  ext = parts[l]
  c(main, ext)
}

fname_attach_ts = function(fname, timestamp=FALSE, ...) {
  parts = fname_split(fname, ...)
  ts = Sys.time()
  if (timestamp) dt = strftime(ts, format='%Y%m%dT%H%M%S%z')
  else dt = strftime(ts, format='%Y%m%dT000000%z')
  paste(paste(parts[1], dt, sep='_'), parts[2], sep='.')
}


init_handler = function(env) {
  function(event) {
    env[['FNAME_FILES']] = list()
    env[['START_TS_FILE']] = Sys.time()
  }
}


send_handler = function(cmd) {
  function(event) {
    event$target$send(cmd)
  }
}


subscribe_handler = function(cmd) {
  function(event) {
    msg = parse_msg_raw(event$data)
    if (!is.null(msg$info)) {
      event$target$send(cmd)
    }
  }
}


mangle_handler = function() {
  function(event) {
    event$timestamp = Sys.time()
  }
}


zip_handler = function(env) {
  function(event) {
    # fname = env[['FNAME_FILE']]
    for (fname in env[['FNAME_FILES']]) {
      if (!is.null(fname))
        zip(paste(fname, 'zip', sep='.'), fname, flags='-r9XmT')
      # r -- recursive, 9 -- compression ratio, m -- remove after zip, T -- test before remove
    }
  }
}


countdown_handler = function(env, ttl) {
  env[['COUNTDOWN_START']] = Sys.time()
  env[['COUNTDOWN_TTL']] = ttl
  function(event) {
    env[['COUNTDOWN_ELIPSED']] = Sys.time() - env[['COUNTDOWN_START']]
    if (env[['COUNTDOWN_ELIPSED']] >= env[['COUNTDOWN_TTL']]) event$target$close()
  }
}


pong_handler = function(ws) {
  function(event) {
    if (event$data == 'ping')
      ws$send('pong')
  }
}


progress_handler = function(max_char=100, level='info') {
  function(event) {
    msg = parse_msg_raw(event$data)
    if (is.infinite(max_char)) max_char = nchar(event$data)
    msg_string = substr(event$data, 1, max_char)

    if (is.null(msg$action)) {  # print notification in full
      print(event$data)
    } else {
      if (level=='info') {
        cat('.')
      }
      if (level=='debug') {  # print everything
        print(msg_string)
      }
    }
  }
}


log_handler = function(env, fname, attach_ts=TRUE, chunk_size=100, interval=10) {
  fname = fname_attach_ts(fname, timestamp=attach_ts, default_ext = 'log')

  env[['FNAME_LOG']] = fname
  env[['ACTIVE_LOG']] = TRUE
  env[['LAST_TS_LOG']] = Sys.time()  # timer
  f = function(event) {
    if (env[['ACTIVE_LOG']]) {
      env[['tmp_log']] = c(env[['tmp_log']], event$data)
      if (length(env[['tmp_log']]) >= chunk_size | (Sys.time() - env[['LAST_TS_LOG']]) > interval) {
        readr::write_lines(x=env[['tmp_log']], path=fname, append=TRUE)
        env[['tmp_log']] = NULL  # truncate instead of drop
        env[['LAST_TS_LOG']] = Sys.time()  # reset timer
      }
    }
  }
  f
}


#' env[['ACTIVE_STREAM']] is the state machine
stream_handler = function(env, buffer=10000) {
  env[['ACTIVE_STREAM']] = FALSE
  function(event) {
    msg = parse_msg_raw(event$data)
    data = parse_msg_data(event$data)
    if (is.null(msg$action)) return()
    if (msg$action == 'partial') {
      env[['ACTIVE_STREAM']] = TRUE
      env[['event']] = list()  # initialise list
      env[['data']] = data[0, ]  # dataframe skeleton
    }
    if (env[['ACTIVE_STREAM']]) {  # TRUE
      env[['event']] = c(env[['event']], list(event))
      env[['event']] = tail(env[['event']], buffer)
      env[['data']] = dplyr::bind_rows(env[['data']], data)  # append data to env[[records]]
      env[['data']] = tail(env[['data']], buffer)  # buffer = keep the latest n records
    }
  }
}


file_handler = function(env, fname, attach_ts=TRUE, append=TRUE, chunk_size=1000, interval=10) {
  fname = fname_attach_ts(fname, timestamp=attach_ts)

  env[['FNAME_FILE']] = fname
  env[['FNAME_FILES']] = c(env[['FNAME_FILES']], fname)
  env[['ACTIVE_FILE']] = FALSE
  env[['LAST_TS_FILE']] = Sys.time()  # timer
  function(event) {
    msg = parse_msg_raw(event$data)
    data = parse_msg_data(event$data)
    if (is.null(msg$action)) return()
    if (msg$action == 'partial') { # skip the first few rows before 'partial' came
      env[['ACTIVE_FILE']] = TRUE
      env[['tmp_data']] = data[0, ]  # dataframe skeleton, to preserve column placeholders
      save_data(data=env[['tmp_data']], fname=fname, append=FALSE)  # write the header
    }
    if (env[['ACTIVE_FILE']]) {
      env[['tmp_data']] = dplyr::bind_rows(env[['tmp_data']], data)
      # Save file if nrow exceeds chunk_size or waited more than 30 seconds (interval)
      if (nrow(env[['tmp_data']]) >= chunk_size | (Sys.time() - env[['LAST_TS_FILE']]) > interval) {
        save_data(data=env[['tmp_data']], fname=fname, append=append)
        env[['tmp_data']] = env[['tmp_data']][0, ]  # truncate instead of drop
        env[['LAST_TS_FILE']] = Sys.time()  # reset timer
      }
    }
  }
}


snapshot_handler = function(env, key) {
  env[['ACTIVE_SNAPSHOT']] = FALSE
  function(event) {
    msg = parse_msg_raw(event$data)
    data = parse_msg_data(event$data)
    if (is.null(msg$action)) return()
    if (msg$action == 'partial')
      env[['ACTIVE_SNAPSHOT']] = TRUE
    if (env[['ACTIVE_SNAPSHOT']]) {
      env[['snapshot']] = ob_increment(snapshot=env[['snapshot']], increment=data, key=key)
    }
  }
}
