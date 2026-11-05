# Consume a message if there is one available

Consume a message if there is one available

## Usage

``` r
try_consume(queue)
```

## Arguments

- queue:

  The queue object.

## Value

A message, or `NULL` if there is not message to work on.

## See also

[liteq](liteq.md) for examples

Other liteq messages: [`ack()`](ack.md), [`consume()`](consume.md),
[`is_empty()`](is_empty.md),
[`list_failed_messages()`](list_failed_messages.md),
[`list_messages()`](list_messages.md),
[`message_count()`](message_count.md), [`publish()`](publish.md),
[`remove_failed_messages()`](remove_failed_messages.md),
[`requeue_failed_messages()`](requeue_failed_messages.md)
