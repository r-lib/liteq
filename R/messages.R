
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
  lock <- message_lock_file(lockdir, queue, id)
  con <- dbConnect(SQLite(), lock)
  db_query(con, "CREATE TABLE foo (id INT)")

  structure(
    list(id = id, title = title, message = message, db = db, lock = con),
    class = "liteq_message"
  )
}

message_lock_file <- function(lockdir, queue, id) {
  file.path(lockdir, paste0(queue, "-", id, ".lock"))
}

#' Publish a message in a queue
#'
#' @param queue The queue object.
#' @param title The title of the message. It can be the empty string.
#' @param message The body of the message. It can be the empty string.
#'
#' @export

publish <- function(queue, title = "", message = "") {
  db_publish(queue$db, queue$name, title, message)
}

#' Consume a message from a queue
#'
#' Blocks and waits for a message if there isn't one to work on currently.
#'
#' @param queue The queue object.
#' @return A message.
#'
#' @export

consume <- function(queue) {
  db_consume(queue$db, queue$name)
}

#' Consume a message if there is one available
#'
#' @param queue The queue object.
#' @return A message, or `NULL` if there is not message to work on.
#'
#' @export

try_consume <- function(queue) {
  db_try_consume(queue$db, queue$name)
}

#' Acknowledge that the work on a message has finished successfully
#'
#' @param message The message object.
#' @export

ack <- function(message) {
  db_ack(message$db, message$queue, message$id, message$lock, TRUE)
}

#' Report that the work on a message has failed
#'
#' @param message The message object.
#' @export

nack <- function(message) {
  db_ack(message$db, message$queue, message$id, message$lock, FALSE)
}
