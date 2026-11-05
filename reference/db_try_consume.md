# Try to consume a message from the queue

If there is a message that it `READY`, it returns that. Otherwise it
checks for crashed workers.

## Usage

``` r
db_try_consume(db, queue, crashed = TRUE, con = NULL)
```

## Arguments

- db:

  DB file name.

- queue:

  Name of the queue.

## Details of the implementation

The database must be locked for the whole operation, including checking
on or creating the lock databases.

1.  If there is a `READY` message, that one is taken.

2.  Otherwise if there are `WORKING` messages, then we check them one by
    one. This might take a lot of time, and the DB must be locked for
    the whole search, so it is not ideal. But I don't have a better
    solution right now.

Taking a message means

1.  Updating its row.status to `WORKING`.

2.  Creating another database that serves as the lock for this message.
