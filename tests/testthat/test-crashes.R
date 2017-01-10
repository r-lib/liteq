
context("crashed consumers")

test_that("crashed consumers", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)
  publish(q, title = title <- "title", message = text <- "MSG")
  msg <- try_consume(q)

  ## now we simulate a crash, so the connection embedded in `msg` is closed
  rm(msg)
  gc()

  ## now, if we try to get a message, the same message must be served again
  msg <- try_consume(q)
  expect_false(is.null(msg))
  if (!is.null(msg)) {
    ack(msg)
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }
})

test_that("multiple crashed consumers", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)
  publish(q, title = "title1", message = "MSG1")
  publish(q, title = "title2", message = "MSG2")
  msg <- try_consume(q)
  msg2 <- try_consume(q)

  ## both crash
  rm(msg, msg2)
  gc()

  ## both are restarted
  msg <- try_consume(q)
  msg2 <- try_consume(q)

  expect_false(is.null(msg))
  if (!is.null(msg)) {
    ack(msg)
    expect_equal(msg$title, "title1")
    expect_equal(msg$message, "MSG1")
  }

  expect_false(is.null(msg2))
  if (!is.null(msg2)) {
    ack(msg2)
    expect_equal(msg2$title, "title2")
    expect_equal(msg2$message, "MSG2")
  }
})
