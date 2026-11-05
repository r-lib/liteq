# Make a message object

It creates the lock for the message as well.

## Usage

``` r
make_message(id, title, message, db, queue, lockdir)
```

## Arguments

- id:

  Message id, integer, auto-generated.

- title:

  Title of message.

- message:

  The message itself.

- db:

  Main DB file.

- queue:

  Name of the queue.

- lockdir:

  Directory to create the message lock in.

## Value

message object

## Details

The message object contains the connection to the message lock. If the
worker crashes, then there will be no reference to the connection, and
the lock will be released. This is how we detect crashed workers.
