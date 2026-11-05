# Requeue failed messages

Requeue failed messages

## Usage

``` r
requeue_failed_messages(queue, id = NULL)
```

## Arguments

- queue:

  The queue object.

- id:

  Ids of the messages to requeue. If it is `NULL`, then all failed
  messages will be requeued.

## See also

[liteq](liteq.md) for examples

Other liteq messages: [`ack()`](ack.md), [`consume()`](consume.md),
[`is_empty()`](is_empty.md),
[`list_failed_messages()`](list_failed_messages.md),
[`list_messages()`](list_messages.md),
[`message_count()`](message_count.md), [`publish()`](publish.md),
[`remove_failed_messages()`](remove_failed_messages.md),
[`try_consume()`](try_consume.md)
