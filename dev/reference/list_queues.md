# List all queues in a database

List all queues in a database

## Usage

``` r
list_queues(db = default_db())
```

## Arguments

- db:

  The queue database to query.

## Value

A list of `liteq_queue` objects.

## See also

[liteq](liteq.md) for examples

Other liteq queues: [`create_queue()`](create_queue.md),
[`delete_queue()`](delete_queue.md), [`ensure_queue()`](ensure_queue.md)
