
#' @importFrom rappdirs user_data_dir
#' @export

default_db <- function() {
  file.path(
    user_data_dir(appname = "liteq"),
    "liteq.db"
  )
}

#' Ensure that the DB exists and has the right columns
#'
#' We try a query, and if it fails then we try to create the DB.
#'
#' @param db DB file.
#'
#' @keywords internal

ensure_db <- function(db) {
  tryCatch(
    do_db(db, "SELECT * FROM meta LIMIT 1"),
    error = function(e) {
      dir.create(dirname(db), recursive = TRUE, showWarnings = FALSE)
      db_create_db(db)
    }
  )
}

db_query <- function(con, query, ...) {
  dbGetQuery(con, sqlInterpolate(con, query, ...))
}

#' @importFrom DBI dbGetQuery sqlInterpolate dbConnect dbDisconnect
#' @importFrom RSQLite SQLite

do_db <- function(db, query, ...) {
  con <- dbConnect(SQLite(), db)
  on.exit(dbDisconnect(con))
  db_query(con, query, ...)
}

db_lock <- function(con) {
  dbGetQuery(con, "PRAGMA busy_timeout = 10000")
  done <- FALSE
  while (!done) {
    tryCatch(
      {
        dbGetQuery(con, "BEGIN EXCLUSIVE")
        done <- TRUE
      },
      error = function(e) NULL
    )
  }
}

db_create_db <- function(db) {
  do_db(
    db,
    "CREATE TABLE meta (
       name TEXT PRIMARY KEY,
       created TIMESTAMP,
       lockdir TEXT
    )"
  )
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
#'
#' @importFrom rappdirs user_cache_dir
#' @keywords internal

db_create_queue <- function(name, db) {
  do_db(
    db,
    'CREATE TABLE ?name (
      id INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      status TEXT DEFAULT "READY"
    );
    INSERT INTO meta (queue, created, lockdir) VALUES
      (?name, DATE("now"), ?lockdir)
    );',
    name = name,
    lockdir = file.path(user_cache_dir(appname = "liteq"))
  )
}

db_list_queues <- function(db) {
  do_db(db, "SELECT name FROM meta");
}

db_publish <- function(db, queue, title, message) {
  do_db(
    db,
    "INSERT INTO ?table (title, message)
     VALUES (?title, ?message)",
    table = queue,
    title = title,
    message = message
  )
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
#' @keywords internal

db_try_consume <- function(db, queue, crashed = TRUE, con = NULL) {
  if (is.null(con)) {
    con <- dbConnect(SQLite(), db)
    on.exit(try_silent(dbDisconnect(con)), add = TRUE)
  }

  ## Lock DB
  db_lock(con)

  ## See if there is a message to work on. If there is, we just return it.
  msg <- db_query(
    con, 'SELECT * FROM ?table WHERE status = "READY" LIMIT 1',
    table = queue
  )
  if (nrow(msg) == 1) {
    db_query(
      con, 'UPDATE ?table SET status = "WORKING" WHERE id = ?id',
      table = queue,
      id = msg$id
    )
    lockdir <- db_query(
      con, "SELECT lockdir FROM meta WHERE name = ?name",
      name = queue
    )$lockdir
    return(make_message(msg$id, msg$title, msg$message, db, queue, lockdir))
  }

  ## Otherwise we need to check on crashed workers
  if (db_clean_crashed(con, queue)) {
    db_try_consume(db, queue, crashed = FALSE, con = con)
  }

  ## The disconnect in on.exit will unlock the DB
}

db_clean_crashed <- function(con, queue) {
  work <- db_query(
    con, 'SELECT * FROM ?table WHERE status = "WORKING"',
    table = queue
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
        lcon <- dbConnect(SQLite(), lock)
        dbGetQuery(lcon, "SELECT * FROM foo")
      },
      error = function(x) "busy"
    )
    if (! identical(x, "busy")) {
      try_silent(dbDisconnect(lcon))
      db_query(
        con, 'UPDATE ?table SET status = "READY" WHERE id = ?id',
        table = queue,
        id = work$id[i]
      )
    }
  }
}

db_consume <- function(db, queue) {
  ## TODO
}

db_ack <- function(db, queue, id, lock, success) {
  con <- dbConnect(SQLite(), db)
  db_lock(con)

}
