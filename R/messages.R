
#' Make a message object
#'
#' It creates the lock for the message as well.
#'
#' The message object contains the connection to the message lock. If the
#' worker crashes, then there will be no reference to the connection, and
#' the lock will be released. This is how we detect crashed workers.
#'
#' @param id Message id, integer, auto-generated.
#' @param title Title of message.
#' @param message The message itself.
#' @param db Main DB file.
#' @param queue Name of the queue.
#' @param lockdir Directory to create the message lock in.
#' @return message object
#'
#' @keywords internal

make_message <- function(id, title, message, db, queue, lockdir) {
  if (is.null(id)) return(NULL)
  dir.create(lockdir, recursive = TRUE, showWarnings = FALSE)
  lock <- message_lock_file(lockdir, queue, id)
  con <- db_connect(lock)
  # Order is important here!
  dbExecute(con, "CREATE TABLE foo (id INT)")
  dbExecute(con, "BEGIN IMMEDIATE")

  structure(
    list(
      id = id,
      title = title,
      message = message,
      db = db, queue = queue,
      lock = con
    ),
    class = "liteq_message"
  )
}

message_lock_file <- function(lockdir, queue, id) {
  file.path(lockdir, paste0(queue, "-", id, ".lock"))
}

#' Publish messages in a queue
#'
#' @param queue The queue object.
#' @param title The title of the messages. It can be the empty string.
#' @param message The body of the messages. It can be the empty string.
#'   Must be the same length as `title`.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

publish <- function(queue, title = "", message = "") {
  assert_that(is_queue(queue))
  assert_that(is.character(title))
  assert_that(is.character(message))
  assert_that(length(title) == length(message))
  db_publish(queue$db, queue$name, title, message)
}

#' Consume a message from a queue
#'
#' Blocks and waits for a message if there isn't one to work on currently.
#'
#' @param queue The queue object.
#' @param poll_interval Poll interval in milliseconds. How often to poll
#'    the queue for new jobs, if none are immediately available.
#' @return A message.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

consume <- function(queue, poll_interval = 500) {
  assert_that(is_queue(queue))
  msg <- db_consume(queue$db, queue$name, poll_interval = poll_interval)
  make_message(msg$msg$id, msg$msg$title, msg$msg$message, msg$db,
               msg$queue, msg$lockdir)
}

#' Consume a message if there is one available
#'
#' @param queue The queue object.
#' @return A message, or `NULL` if there is not message to work on.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

try_consume <- function(queue) {
  assert_that(is_queue(queue))
  msg <- db_try_consume(queue$db, queue$name)
  make_message(msg$msg$id, msg$msg$title, msg$msg$message, msg$db,
               msg$queue, msg$lockdir)
}

#' Acknowledge that the work on a message has finished successfully
#'
#' @param message The message object.
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

ack <- function(message) {
  assert_that(is_message(message))
  db_ack(message$db, message$queue, message$id, message$lock, TRUE)
}

#' Report that the work on a message has failed
#'
#' @param message The message object.
#' @seealso [liteq] for examples
#' @export

nack <- function(message) {
  assert_that(is_message(message))
  db_ack(message$db, message$queue, message$id, message$lock, FALSE)
}

#' @export

print.liteq_message <- function(x, ...) {
  cat("liteq message from queue ", sQuote(x$queue), ":\n", sep = "")
  msg_bytes <- nchar(x$message, type = "bytes")
  cat("  ", x$title, " (", msg_bytes, " B)\n", sep = "")
  invisible(x)
}

#' Get the number of messages in a queue.
#'
#' @param queue The queue object.
#' @return Number of messages in the queue.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

message_count <- function(queue) {
  assert_that(is_queue(queue))
  db_message_count(queue$db, queue$name)
}

#' Check if a queue is empty
#'
#' @param queue The queue object.
#' @return Logical, whether the queue is empty.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

is_empty <- function(queue) {
  assert_that(is_queue(queue))
  db_is_empty(queue$db, queue$name)
}

#' List all messages in a queue
#'
#' @param queue The queue object.
#' @return Data frame with columns: `id`, `title`, `status`.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

list_messages <- function(queue) {
  assert_that(is_queue(queue))
  db_list_messages(queue$db, queue$name)
}

#' List failed messages in a queue
#'
#' @param queue The queue object.
#' @return Data frame with columns: `id`, `title`, `status`.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

list_failed_messages <- function(queue) {
  assert_that(is_queue(queue))
  db_list_messages(queue$db, queue$name, failed = TRUE)
}

#' Requeue failed messages
#'
#' @param queue The queue object.
#' @param id Ids of the messages to requeue. If it is `NULL`, then all
#'   failed messages will be requeued.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

requeue_failed_messages <- function(queue, id = NULL) {
  assert_that(is_queue(queue))
  assert_that(is_message_ids_or_null(id))
  db_requeue_failed_messages(queue$db, queue$name, id)
}

#' Remove failed messages from the queue
#'
#' @param queue The queue object.
#' @param id Ids of the messages to requeue. If it is `NULL`, then all
#'   failed messages will be removed.
#'
#' @family liteq messages
#' @seealso [liteq] for examples
#' @export

remove_failed_messages <- function(queue, id = NULL) {
  assert_that(is_queue(queue))
  assert_that(is_message_ids_or_null(id))
  db_remove_failed_messages(queue$db, queue$name, id)
}
