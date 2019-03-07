
# 1.0.1.9000

* `consume()` now has a `poll_interval` argument to set how often to poll
  the queue for new jobs.

* New `is_empty()` and `message_count()` functions (#18, @wlandau).

* Get rid of annoying warning about closing unused connections
  (#15, #20, @wlandau).

# 1.0.1

* Set the `LITEQ_CACHE_DIR` environment variable to change the
  default cache directory.

# 1.0.0

First public release.
