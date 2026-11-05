# Positive or negative ackowledgement

If positive, then we need to remove the message from the queue. If
negative, we just set the status to `FAILED`.

## Usage

``` r
db_ack(db, queue, id, lock, success)
```

## Arguments

- db:

  DB file.

- queue:

  Queue name.

- id:

  Message id.

- lock:

  Name of the message lock file.

- success:

  Whether this is a positive or negative ACK.
