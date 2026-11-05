# Make sure that a queue exists

If it does not exist, then the queue will be created.

## Usage

``` r
ensure_queue(name, db = default_db(), crash_strategy = "fail")
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

## Value

The queue object.

## See also

[liteq](liteq.md) for examples

Other liteq queues: [`create_queue()`](create_queue.md),
[`delete_queue()`](delete_queue.md), [`list_queues()`](list_queues.md)
