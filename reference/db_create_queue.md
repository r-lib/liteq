# Create a queue

The database columns:

- id Id of the message, it is generated automatically by the database.

- title The title of the message, can be empty. In the future, it can be
  used to filter messages.

- message The message, arbitrary text, can be empty.

- status Can be:

  - `READY`, ready to be consumed

  - `WORKING`, it is being consumed

  - `FAILED`, failed.

- requeued How many times the message was requeued.

## Usage

``` r
db_create_queue(name, db, crash_strategy)
```

## Arguments

- name:

  Name of the queue. If not specified or `NULL`, a name is generated
  randomly.

- db:

  Path to the database file.

- crash_strategy:

  What to do with crashed jobs. The default is that they will `"fail"`
  (just like a negative acknowledgement). Another possibility is
  `"requeue"`, in which case they are requeued immediately, potentially
  even multiple times. Alternatively it can be a number, in which case
  they are requeued at most the specified number of times.
