
#' Lightweight Portable Message Queue Using 'SQLite'
#'
#' Message queues for R. Built on top of 'SQLite' databases.
#'
#' @docType package
#' @name liteq
#' @examples
#' # A temporary queue
#' db <- tempfile()
#' q <- ensure_queue("jobs", db = db)
#' q
#' list_queues(db)
#'
#' # Publish two messages
#' publish(q, title = "First message", message = "Hello world!")
#' publish(q, title = "Second message", message = "Hello again!")
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
#' try_consume(q)

NULL
