
#' Create a queue in a database
#'
#' It also creates the database, if it does not exist.
#' @param name Name of the queue. If not specified or `NULL`, a
#'   name is generated randomly.
#' @param db Path to the database file.
#' @param crash_strategy What to do with crashed jobs. The default is that
#'   they will `"fail"` (just like a negative acknowledgement). Another
#'   possibility is `"requeue"`, in which case they are requeued
#'   immediately, potentially even multiple times. Alternatively it can be
#'   a number, in which case they are requeued at most the specified number
#'   of times.
#'
#' @family liteq queues
#' @seealso [liteq] for examples
#' @export

create_queue <- function(name = NULL, db = default_db(),
                         crash_strategy = "fail") {

  assert_crash_strategy(crash_strategy)

  name <- name %||% random_queue_name()

  ensure_db(db)
  db_create_queue(name, db, crash_strategy)

  make_queue(name, db)
}

#' Delete a queue
#'
#' @param queue The queue to delete.
#' @param force Whether to delete the queue even if it contains messages.
#'
#' @family liteq queues
#' @seealso [liteq] for examples
#' @export

delete_queue <- function(queue, force = FALSE) {
  ## TODO
}

#' Make sure that a queue exists
#'
#' If it does not exist, then the queue will be created.
#'
#' @inheritParams create_queue
#' @return The queue object.
#'
#' @family liteq queues
#' @seealso [liteq] for examples
#' @export

ensure_queue <- function(name, db = default_db(),
                         crash_strategy = "fail") {
  assert_crash_strategy(crash_strategy)
  ensure_db(db)
  db_ensure_queue(name, db, crash_strategy)
  make_queue(name, db)
}

#' List all queues in a database
#'
#' @param db The queue database to query.
#' @return A list of `liteq_queue` objects.
#'
#' @family liteq queues
#' @seealso [liteq] for examples
#' @export

list_queues <- function(db = default_db()) {
  ensure_db(db)
  lapply(db_list_queues(db)$name, make_queue, db = db)
}

make_queue <- function(name, db) {
  structure(
    list(name = name, db = db),
    class = "liteq_queue"
  )
}

#' @export

print.liteq_queue <- function(x, ...) {
  cat("liteq queue ", sQuote(x$name), "\n", sep = "")
  invisible(x)
}
