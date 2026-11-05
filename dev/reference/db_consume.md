# Consume a message from a message queue

This is the blocking version of [`try_consume()`](try_consume.md).
Currently it just polls twice a second, and sleeps between the polls.
Each poll will also trigger a crash cleanup, if there are workers
running.

## Usage

``` r
db_consume(db, queue, poll_interval = 500)
```

## Arguments

- queue:

  The queue object.
