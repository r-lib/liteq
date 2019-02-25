
#' Lightweight Portable Message Queue Using 'SQLite'
#'
#' Message queues for R. Built on top of 'SQLite' databases.
#'
#' @docType package
#' @name liteq
#' @section Examples:
#' ```
#' # We don't run this, because it write to the cache directory
#' db <- tempfile()
#' q <- ensure_queue("jobs", db = db)
#' q
#' list_queues(db)
#'
#' # Publish two messages
#' publish(q, title = "First message", message = "Hello world!")
#' publish(q, title = "Second message", message = "Hello again!")
#' is_empty(q)
#' message_count(q)
#' list_messages(q)
#'
#' # Consume one
#' msg <- try_consume(q)
#' msg
#'
#' ack(msg)
#' list_messages(q)
#' msg2 <- try_consume(q)
#' nack(msg2)
#' list_messages(q)
#'
#' # No more messages
#' is_empty(q)
#' try_consume(q)
#' ```
#'
#' @examples
#' ## See the manual page

NULL
