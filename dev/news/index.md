# Changelog

## liteq (development version)

## liteq 1.2.0

CRAN release: 2026-11-05

- [`publish()`](../reference/publish.md) now accepts multiple messages
  (note that `title` and `message` must be of the same length) and
  publishes them in a single transaction
  ([\#25](https://github.com/r-lib/liteq/issues/25),
  [@Enchufa2](https://github.com/Enchufa2)).

- Setting the `R_LITEQ_BUSY_TIMEOUT` environment variable now works
  properly. Previously it was ignored and the timeout was set to ten
  seconds ([\#24](https://github.com/r-lib/liteq/issues/24),
  [@Enchufa2](https://github.com/Enchufa2)).

- liteq now works better with many concurrent workers, and fails much
  less often with “database is locked” errors
  ([\#28](https://github.com/r-lib/liteq/issues/28),
  [\#29](https://github.com/r-lib/liteq/issues/29),
  [@Enchufa2](https://github.com/Enchufa2)):

  - Checking for crashed workers does not wait for the busy timeout on
    each message that is being worked on. Previously, this kept the
    database locked for a long time, for the busy timeout times the
    number of active workers.

  - All write transactions now use `BEGIN IMMEDIATE`. This avoids
    deadlocks between writers, and readers
    (e.g. [`list_messages()`](../reference/list_messages.md)) are not
    blocked while another process is writing.

  - All write operations, including
    [`ensure_queue()`](../reference/ensure_queue.md) and
    [`publish()`](../reference/publish.md), now keep waiting for the
    database lock, like [`consume()`](../reference/consume.md) and
    [`ack()`](../reference/ack.md) already did. Previously they failed
    if they did not get the lock within the busy timeout.

## liteq 1.1.0

CRAN release: 2019-03-08

- Work around a SQLITE bug that resets the database busy timeout after a
  successful query. Now we set the timeout after each database
  operation. The timeout can now also be set via the
  `R_LITEQ_BUSY_TIMEOUT` environment variable, and it defaults to 10
  seconds, instead of 1 second.

- [`consume()`](../reference/consume.md) now has a `poll_interval`
  argument to set how often to poll the queue for new jobs.

- New [`is_empty()`](../reference/is_empty.md) and
  [`message_count()`](../reference/message_count.md) functions
  ([\#18](https://github.com/r-lib/liteq/issues/18),
  [@wlandau](https://github.com/wlandau)).

- Get rid of annoying warning about closing unused connections
  ([\#15](https://github.com/r-lib/liteq/issues/15),
  [\#20](https://github.com/r-lib/liteq/issues/20),
  [@wlandau](https://github.com/wlandau)).

## liteq 1.0.1

CRAN release: 2017-10-20

- Set the `LITEQ_CACHE_DIR` environment variable to change the default
  cache directory.

## liteq 1.0.0

CRAN release: 2017-01-27

First public release.
