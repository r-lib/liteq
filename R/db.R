
# -------------------------------------------------------------------------
# Internals to DB
# -------------------------------------------------------------------------

#' Perform a write transaction
#'
#' We use immediate transactions to avoid deadlocks.
#' @param conn Connection.
#' @param code Code to run.
#' @return Return value of `code`, visibility is lost.
#'
#' @noRd
#' @importFrom DBI dbExecute dbRollback

dbWithWriteTransaction <- function(conn, code) {
  dbExecute(conn, "BEGIN IMMEDIATE")
  rollback <- function(e) {
    call <- dbRollback(conn)
    if (identical(call, FALSE)) {
      stop(paste(
        "Failed to rollback transaction.",
        "Tried to roll back because an error occurred:",
        conditionMessage(e)
      ), call. = FALSE)
    }
    if (inherits(e, "error")) stop(e)
  }
  tryCatch(
    {
      res <- force(code)
      dbExecute(conn, "COMMIT")
      res
    },
    db_abort = rollback,
    error = rollback,
    interrupt = rollback
  )
}

#' @importFrom DBI dbExecute

db_set_timeout <- function(con) {
  timeout <- as.integer(Sys.getenv("R_LITEQ_BUSY_TIMEOUT", "10000"))
  if (is.na(timeout)) timeout <- 10000
  dbExecute(con, sprintf("PRAGMA busy_timeout = %d", timeout))
}

#' Always use `db_connect()` to connect to the DB
#'
#' Because it sets up the busy timeout.
#'
#' This is DB internal, but also called when creating the message lock.
#'
#' @param ... Passed to [DBI::dbConnect()].
#' @param synchronous Passed to [DBI::dbConnect()].
#' @return Connection.
#'
#' @noRd
#' @importFrom RSQLite SQLite
#' @importFrom DBI dbConnect

db_connect <- function(..., synchronous = NULL) {
  con <- dbConnect(SQLite(), synchronous = synchronous, ...)
  db_set_timeout(con)
  con
}

db_queue_name <- function(name) {
  paste0("qq", name)
}

#' Use this instead of [DBI::dbGetQuery()]
#'
#' Because it does interpolation.
#'
#' @param con Connection.
#' @param query String, db query.
#' @param ... Passed to [DBI::sqlInterpolate()].
#' @return Return value of [DBI::dbGetQuery()].
#'
#' @noRd
#' @importFrom DBI dbGetQuery sqlInterpolate

db_query <- function(con, query, ...) {
  dbGetQuery(con, sqlInterpolate(con, query, ...))
}

#' Use this instead of [DBI::dbExecute()]
#'
#' Because it does interpolation.
#'
#' @param con Connection.
#' @param query String, db query.
#' @param ... Passed to [DBI::sqlInterpolate()].
#' @return Return value of [DBI::dbExecute()].
#'
#' @noRd
#' @importFrom DBI dbExecute

db_execute <- function(con, query, ...) {
  dbExecute(con, sqlInterpolate(con, query, ...))
}

#' Use this for _read_ transactions
#'
#' @param db Path to DB.
#' @param query Query string.
#' @param ... Passed to [db_query()].
#' @return From [db_query()].
#'
#' @noRd
#' @importFrom DBI dbDisconnect dbWithTransaction

do_db_read <- function(db, query, ...) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con))
  dbWithTransaction(con, {
    db_query(con, query, ...)
  })
}

#' Use this for _write_ transactions
#'
#' @param db Path to DB.
#' @param query Query string.
#' @param ... Passed to [db_query()].
#' @return From [db_query()].
#'
#' @noRd
#' @importFrom DBI dbDisconnect

do_db_write <- function(db, query, ...) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con))
  dbWithWriteTransaction(con, {
    db_execute(con, query, ...)
  })
}

db_create_db <- function(db) {
  do_db_write(
    db,
    "CREATE TABLE IF NOT EXISTS meta (
       name TEXT PRIMARY KEY,
       created TIMESTAMP,
       lockdir TEXT,
       requeue TEXT DEFAULT \"fail\"   -- fail/ requeue/ number of requeues
    )"
  )
}

db_create_queue_locked <- function(db, con, name, crash_strategy) {
  db_execute(
    con,
    'CREATE TABLE ?tablename (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      status TEXT DEFAULT "READY",
      requeued INTEGER DEFAULT 0)',
    tablename = db_queue_name(name)
  )
  db_execute(
    con,
    'INSERT INTO meta (name, created, lockdir, requeue) VALUES
      (?name, DATE("now"), ?lockdir, ?crash)',
    name = name,
    lockdir = db_lockdir(db),
    crash = as.character(crash_strategy)
  )
}

db_lockdir <- function(db) {
  file.path(
    Sys.getenv("LITEQ_CACHE_DIR", user_cache_dir(appname = "liteq")),
    paste0(basename(db), "-", random_lock_name())
  )
}

db_try_consume_locked <- function(db, queue, con, crashed = TRUE) {

  ## See if there is a message to work on. If there is, we just return it.
  msg <- db_query(
    con, 'SELECT * FROM ?tablename WHERE status = "READY" LIMIT 1',
    tablename = db_queue_name(queue)
  )
  if (nrow(msg) == 1) {
    db_execute(
      con, 'UPDATE ?tablename SET status = "WORKING" WHERE id = ?id',
      tablename = db_queue_name(queue),
      id = msg$id
    )
    lockdir <- db_query(
      con, "SELECT lockdir FROM meta WHERE name = ?name",
      name = queue
    )$lockdir

    return(list(msg = msg, db = db, queue = queue, lockdir = lockdir))
  }

  ## Otherwise we need to check on crashed workers
  if (crashed && db_clean_crashed(con, queue)) {
    mmsg <- db_try_consume_locked(db, queue, con, crashed = FALSE)
    return(mmsg)
  }
}

#' @importFrom DBI dbDisconnect

db_clean_crashed <- function(con, queue) {
  work <- db_query(
    con, 'SELECT * FROM ?tablename WHERE status = "WORKING"',
    tablename = db_queue_name(queue)
  )
  if (nrow(work) == 0) return(FALSE)

  meta <- db_query(
    con, "SELECT * FROM meta WHERE name = ?name",
    name = queue
  )

  locks <- message_lock_file(meta$lockdir, queue, work$id)
  for (i in seq_along(locks)) {
    lock <- locks[[i]]
    x <- tryCatch(
      {
        lcon <- db_connect(lock)
        dbGetQuery(lcon, "SELECT * FROM foo")
      },
      error = function(x) "busy"
    )
    if (! identical(x, "busy")) {
      try_silent(dbDisconnect(lcon))
      if (meta$requeue == "fail" || meta$requeue == "requeue") {
        ## Always fail, or always requeue
        status <- if (meta$requeue == "fail") "FAILED" else "READY"
        db_clean_crashed_update(con, queue, work$id[i], status)

      } else if (as.numeric(work$requeued[i]) >= as.numeric(meta$requeue)) {
        ## Requeued too many times
        db_clean_crashed_update(con, queue, work$id[i], "FAILED")

      } else {
        ## Can still requeue
        db_clean_crashed_update(con, queue, work$id[i], "READY")
        db_execute(
          con,
          'UPDATE ?tablename SET requeued = requeued + 1 WHERE id = ?id',
          tablename = db_queue_name(queue),
          id = work$id[i]
        )
      }
      unlink(lock)
    }
  }
  TRUE
}

db_clean_crashed_update <- function(con, queue, id, status) {
  db_execute(
    con, 'UPDATE ?tablename SET status = ?status WHERE id = ?id',
    tablename = db_queue_name(queue),
    id = id,
    status = status
  )
}

db_requeue_all_failed_messages <- function(db, queue) {
  do_db_write(
    db,
    "UPDATE ?tablename SET status = \"READY\" WHERE status = \"FAILED\"",
    tablename = db_queue_name(queue)
  )
}

db_requeue_some_failed_messages <- function(db, queue, id) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con))
  dbWithWriteTransaction(con, {
    for (id1 in id) {
      db_execute(
        con,
        "UPDATE ?tablename
         SET status = \"READY\"
         WHERE status = \"FAILED\" AND id = ?id",
        tablename = db_queue_name(queue),
        id = id1
      )
    }
  })
}

db_remove_all_failed_messages <- function(db, queue) {
  do_db_write(
    db,
    "DELETE FROM ?tablename WHERE status = \"FAILED\"",
    tablename = db_queue_name(queue)
  )
}

db_remove_some_failed_messages <- function(db, queue, id) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con))
  dbWithWriteTransaction(con, {
    for (id1 in id) {
      db_execute(
        con,
        "DELETE FROM ?tablename
         WHERE status = \"FAILED\" AND id = ?id",
        tablename = db_queue_name(queue),
        id = id1
      )
    }
  })
}

# -------------------------------------------------------------------------
# Internals to the package, queues
# -------------------------------------------------------------------------

#' Ensure that the DB exists and has the right columns
#'
#' We try a query, and if it fails then we try to create the DB.
#'
#' @param db DB file.
#'
#' @noRd

ensure_db <- function(db) {
  dir.create(dirname(db), recursive = TRUE, showWarnings = FALSE)
  db_create_db(db)
}

#' @importFrom RSQLite dbExistsTable dbDisconnect

db_ensure_queue <- function(name, db, crash_strategy) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con), add = TRUE)
  dbWithWriteTransaction(con, {
    tablename <- db_queue_name(name)
    if (!dbExistsTable(con, tablename)) {
      db_create_queue_locked(db, con, name, crash_strategy)
    }
  })
}

#' Create a queue
#'
#' The database columns:
#' * id Id of the message, it is generated automatically by the database.
#' * title The title of the message, can be empty. In the future,
#'     it can be used to filter messages.
#' * message The message, arbitrary text, can be empty.
#' * status Can be:
#'   * `READY`, ready to be consumed
#'   * `WORKING`, it is being consumed
#'   * `FAILED`, failed.
#' * requeued How many times the message was requeued.
#'
#' @inheritParams create_queue
#' @importFrom rappdirs user_cache_dir
#' @noRd
#' @importFrom DBI dbDisconnect

db_create_queue <- function(name, db, crash_strategy) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con), add = TRUE)
  dbWithWriteTransaction(con, {
    db_create_queue_locked(db, con, name, crash_strategy)
  })
}

db_list_queues <- function(db) {
  do_db_read(db, "SELECT name FROM meta");
}

db_delete_queue <- function(db, queue, force) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con))
  dbWithWriteTransaction(con, {
    num <- db_query(
      con,
      "SELECT COUNT(*) FROM ?tablename",
      tablename = db_queue_name(queue)
    )

    if (num > 0 && ! force) {
      stop("Unwilling to delete non-empty queue, consider 'force = TRUE'")
    }

    db_execute(
      con,
      "DELETE FROM meta WHERE name = ?name",
      name = queue
    )
    db_execute(
      con,
      "DROP TABLE ?tablename",
      tablename = db_queue_name(queue)
    )
  })
}

# -------------------------------------------------------------------------
# Internals to the package, messages
# -------------------------------------------------------------------------

#' @importFrom DBI dbDisconnect

db_publish <- function(db, queue, title, message) {
  con <- db_connect(db)
  on.exit(dbDisconnect(con))
  dbWithWriteTransaction(con, {
    for (i in seq_along(title)) {
      db_execute(
        con,
        "INSERT INTO ?tablename (title, message)
         VALUES (?title, ?message)",
        tablename = db_queue_name(queue),
        title = title[i],
        message = message[i]
      )
    }
  })
  invisible()
}

#' Try to consume a message from the queue
#'
#' If there is a message that it `READY`, it returns that. Otherwise it
#' checks for crashed workers.
#'
#' @section Details of the implementation:
#'
#' The database must be locked for the whole operation, including
#' checking on or creating the lock databases.
#'
#' 1. If there is a `READY` message, that one is taken.
#' 2. Otherwise if there are `WORKING` messages, then
#'    we check them one by one. This might take a lot of
#'    time, and the DB must be locked for the whole search,
#'    so it is not ideal. But I don't have a better solution
#'    right now.
#'
#' Taking a message means
#' 1. Updating its row.status to `WORKING`.
#' 2. Creating another database that serves as the lock for this message.
#'
#' @param db DB file name.
#' @param queue Name of the queue.
#' @noRd
#' @importFrom DBI dbDisconnect

db_try_consume <- function(db, queue, crashed = TRUE, con = NULL) {
  con <- db_connect(db)
  on.exit(try_silent(dbDisconnect(con)), add = TRUE)
  dbWithWriteTransaction(con, {
    db_try_consume_locked(db, queue, con, crashed)
  })
}

#' Consume a message from a message queue
#'
#' This is the blocking version of [try_consume()]. Currently it just
#' polls twice a second, and sleeps between the polls. Each poll will also
#' trigger a crash cleanup, if there are workers running.
#'
#' @inheritParams try_consume
#'
#' @noRd

db_consume <- function(db, queue, poll_interval = 500) {
  while (TRUE) {
    msg <- db_try_consume(db, queue)
    if (!is.null(msg)) break
    Sys.sleep(poll_interval / 1000)
  }
  msg
}

#' Positive or negative ackowledgement
#'
#' If positive, then we need to remove the message from the queue.
#' If negative, we just set the status to `FAILED`.
#'
#' @param db DB file.
#' @param queue Queue name.
#' @param id Message id.
#' @param lock Name of the message lock file.
#' @param success Whether this is a positive or negative ACK.
#'
#' @noRd

db_ack <- function(db, queue, id, lock, success) {
  con <- db_connect(db)
  on.exit(try_silent(dbDisconnect(con)), add = TRUE)
  dbWithWriteTransaction(con, {
    if (success) {
      num <- db_execute(
        con, "DELETE FROM ?tablename WHERE id = ?id",
        tablename = db_queue_name(queue), id = id
      )

    } else {
      num <- db_execute(
        con, 'UPDATE ?tablename SET status = "FAILED" WHERE id = ?id',
        tablename = db_queue_name(queue), id = id
      )
    }

    if (num == 0) stop("Message does not exist, internal error?")
    if (num > 1) stop("Multiple messages with the same id, internal error")

    lockdir <- db_query(
      con, "SELECT lockdir FROM meta WHERE name = ?name",
      name = queue
    )$lockdir

    try_silent(dbDisconnect(lock))
    lock <- message_lock_file(lockdir, queue, id)
    unlink(lock)
  })

  invisible()
}

db_message_count <- function(db, queue, failed = FALSE) {

  q <- "SELECT COUNT(id) FROM ?tablename LIMIT 1"
  if (failed) q <- paste(q, "WHERE status = \"FAILED\"")

  do_db_read(db, q, tablename = db_queue_name(queue))[1, 1]
}

db_is_empty <- function(db, queue, failed = FALSE) {
  db_message_count(db = db, queue = queue, failed = failed) < 1
}

db_list_messages <- function(db, queue, failed = FALSE) {

  q <- "SELECT id, title, status FROM ?tablename"
  if (failed) q <- paste(q, "WHERE status = \"FAILED\"")

  do_db_read(db, q, tablename = db_queue_name(queue))
}

db_requeue_failed_messages <- function(db, queue, id) {
  if (is.null(id)) {
    db_requeue_all_failed_messages(db, queue)
  } else {
    db_requeue_some_failed_messages(db, queue, id)
  }
  invisible()
}

db_remove_failed_messages <- function(db, queue, id) {
  if (is.null(id)) {
    db_remove_all_failed_messages(db, queue)
  } else {
    db_remove_some_failed_messages(db, queue, id)
  }
  invisible()
}

# -------------------------------------------------------------------------
# Public package API
# -------------------------------------------------------------------------

#' The name of the default database
#'
#' If the queue database is not specified explicitly,
#' then `liteq` uses this file. Its location is determined via the
#' `rappdirs` package, see [rappdirs::user_data_dir()].
#'
#' @return A characater scalar, the name of the default database.
#'
#' @importFrom rappdirs user_data_dir
#' @export

default_db <- function() {
  file.path(
    user_data_dir(appname = "liteq"),
    "liteq.db"
  )
}
