
# liteq 1.2.0

* `publish()` now accepts multiple messages (note that `title` and
  `message` must be of the same length) and publishes them in a single
  transaction (#25, @Enchufa2).

* Setting the `R_LITEQ_BUSY_TIMEOUT` environment variable now works
  properly. Previously it was ignored and the timeout was set to
  ten seconds (#24, @Enchufa2).

* liteq now works better with many concurrent workers, and fails much
  less often with "database is locked" errors (#28, #29, @Enchufa2):

  - Checking for crashed workers does not wait for the busy timeout on
    each message that is being worked on. Previously, this kept the
    database locked for a long time, for the busy timeout times the
    number of active workers.

  - All write transactions now use `BEGIN IMMEDIATE`. This avoids
    deadlocks between writers, and readers (e.g. `list_messages()`) are
    not blocked while another process is writing.

  - All write operations, including `ensure_queue()` and `publish()`,
    now keep waiting for the database lock, like `consume()` and
    `ack()` already did. Previously they failed if they did not get the
    lock within the busy timeout.

# liteq 1.1.0

* Work around a SQLITE bug that resets the database busy timeout after a
  successful query. Now we set the timeout after each database operation.
  The timeout can now also be set via the `R_LITEQ_BUSY_TIMEOUT`
  environment variable, and it defaults to 10 seconds, instead of 1 second.

* `consume()` now has a `poll_interval` argument to set how often to poll
  the queue for new jobs.

* New `is_empty()` and `message_count()` functions (#18, @wlandau).

* Get rid of annoying warning about closing unused connections
  (#15, #20, @wlandau).

# liteq 1.0.1

* Set the `LITEQ_CACHE_DIR` environment variable to change the
  default cache directory.

# liteq 1.0.0

First public release.
