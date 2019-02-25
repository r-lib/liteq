
context("messages")

test_that("publish, is_empty, and message_count", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)
  expect_true(is_empty(q))
  expect_equal(message_count(q), 0)

  for (i in 1:10) {
    publish(q, title = title <- as.character(i), message = text <- "MSG")
  }
  expect_false(is_empty(q))
  expect_equal(message_count(q), 10)
})

test_that("publish & consume", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)

  for (i in 1:10) {
    publish(q, title = title <- as.character(i), message = text <- "MSG")
    msg <- try_consume(q)
    ack(msg)
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }

  for (i in 1:10) {
    publish(q, title = as.character(i), message = paste0("MSG-", i))
  }

  for (i in 1:10) {
    msg <- try_consume(q)
    ack(msg)
    expect_equal(msg$title, as.character(i))
    expect_equal(msg$message, paste0("MSG-", i))
  }
})

test_that("nack", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)

  for (i in 1:10) {
    publish(q, title = title <- as.character(i), message = text <- "MSG")
    msg <- try_consume(q)
    nack(msg)
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }

  ## Check that the messages are still there, but "FAILED"
  con <- db_connect(q$db)
  on.exit(dbDisconnect(con), add = TRUE)
  msgs <- db_query(
    con,
    "SELECT * FROM ?tablename",
    tablename = db_queue_name(q$name)
  )
  expect_equal(msgs$title, as.character(1:10))
  expect_equal(msgs$status, rep("FAILED", 10))
})

test_that("try_consume if queue is empty", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)

  expect_null(try_consume(q))

  for (i in 1:10) {
    publish(q, title = title <- as.character(i), message = text <- "MSG")
    msg <- try_consume(q)
    nack(msg)
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }

  expect_null(try_consume(q))
})
