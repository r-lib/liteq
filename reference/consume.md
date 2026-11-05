# Consume a message from a queue

Blocks and waits for a message if there isn't one to work on currently.

## Usage

``` r
consume(queue, poll_interval = 500)
```

## Arguments

- queue:

  The queue object.

- poll_interval:

  Poll interval in milliseconds. How often to poll the queue for new
  jobs, if none are immediately available.

## Value

A message.

## See also

[liteq](liteq.md) for examples

Other liteq messages: [`ack()`](ack.md), [`is_empty()`](is_empty.md),
[`list_failed_messages()`](list_failed_messages.md),
[`list_messages()`](list_messages.md),
[`message_count()`](message_count.md), [`publish()`](publish.md),
[`remove_failed_messages()`](remove_failed_messages.md),
[`requeue_failed_messages()`](requeue_failed_messages.md),
[`try_consume()`](try_consume.md)
