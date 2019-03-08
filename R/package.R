
#' Lightweight Portable Message Queue Using 'SQLite'
#'
#' Message queues for R. Built on top of 'SQLite' databases.
#'
#' @section Concurrency:
#'
#' liteq works with multiple producer and/or consumer processes accessing
#' the same queue, via the locking mechanism of 'SQLite'. If a queue is
#' locked by 'SQLite', the process that tries to access it, must wait until
#' it is unlocked. The maximum amount of waiting time is by default 10
#' seconds, and it can be changed via the `R_LITEQ_BUSY_TIMEOUT`
#' environment variable, in milliseconds. If you have many concurrent
#' processes using the same liteq database, and see `database locked`
#' errors, then you can try to increase the timeout value.
#'
#' @docType package
#' @name liteq
#' @section Examples:
#' ```
#' # We don't run this, because it writes to the cache directory
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
