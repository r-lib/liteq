# Start a write transaction

We use `BEGIN IMMEDIATE` for every write. A deferred transaction (plain
`BEGIN`) that reads first and writes later might fail with "database is
locked" right away, without waiting for the busy timeout, because SQLite
cannot wait there without risking a deadlock. Unlike `BEGIN EXCLUSIVE`,
`BEGIN IMMEDIATE` does not block readers.

## Usage

``` r
db_lock(con, retry = TRUE)
```

## Arguments

- con:

  DB connection.

- retry:

  Whether to retry if the database is locked.

## Details

If `retry` is `TRUE`, then we keep trying until we get the lock. Each
try waits for the busy timeout, so this does not spin.
