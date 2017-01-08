
#' Create a queue in a database
#'
#' It also creates the database, if it does not exist.
#' @param name Name of the queue. If not specified or `NULL`, a
#'   name is generated randomly.
#' @param db Path to the database file.
#'
#' @export

create_queue <- function(name = NULL, db = default_db()) {

  name <- name %||% random_queue_name()

  ensure_db(db)
  db_create_queue(name, db)

  make_queue(name, db)
}

#' Delete a queue
#'
#' @param queue The queue to delete.
#' @param force Whether to delete the queue even if it contains messages.
#'
#' @export

delete_queue <- function(queue, force = FALSE) {
  ## TODO
}

#' List all queues in a database
#'
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
