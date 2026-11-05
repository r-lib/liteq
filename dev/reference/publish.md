# Publish messages in a queue

Publish messages in a queue

## Usage

``` r
publish(queue, title = "", message = "")
```

## Arguments

- queue:

  The queue object.

- title:

  The title of the messages. It can be the empty string.

- message:

  The body of the messages. It can be the empty string. Must be the same
  length as `title`.

## See also

[liteq](liteq.md) for examples

Other liteq messages: [`ack()`](ack.md), [`consume()`](consume.md),
[`is_empty()`](is_empty.md),
[`list_failed_messages()`](list_failed_messages.md),
[`list_messages()`](list_messages.md),
[`message_count()`](message_count.md),
[`remove_failed_messages()`](remove_failed_messages.md),
[`requeue_failed_messages()`](requeue_failed_messages.md),
[`try_consume()`](try_consume.md)
