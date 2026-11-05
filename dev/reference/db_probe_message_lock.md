# Check if the worker holding a message lock is still alive

A live worker holds an exclusive lock on the lock file, so reading it
fails. We must not wait for the busy timeout here: we hold the write
lock of the main DB, and every other client would have to wait, too.

## Usage

``` r
db_probe_message_lock(lock)
```

## Arguments

- lock:

  Path to the message lock file.

## Value

`"busy"` if the worker is alive.
