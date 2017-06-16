
context("crashed consumers")

test_that("requeueing crashed consumers", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db, crash_strategy = "requeue")
  publish(q, title = title <- "title", message = text <- "MSG")
  msg <- try_consume(q)

  ## now we simulate a crash, so the connection embedded in `msg` is closed
  rm(msg)
  suppressWarnings(gc())

  ## now, if we try to get a message, the same message must be served again
  msg <- try_consume(q)
  expect_false(is.null(msg))
  if (!is.null(msg)) {
    ack(msg)
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }
})

test_that("requeueing multiple crashed consumers", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db, crash_strategy = "requeue")
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

test_that("failing crashed consumers", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db, crash_strategy = "fail")
  publish(q, title = title <- "title", message = text <- "MSG")
  msg <- try_consume(q)

  ## now we simulate a crash, so the connection embedded in `msg` is closed
  rm(msg)
  gc()

  msg <- try_consume(q)
  expect_null(msg)
  fail <- list_failed_messages(q)
  expect_equal(fail$title, title)
})

test_that("requeueing crashed consumers a limited number of times", {
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db, crash_strategy = 2)
  publish(q, title = title <- "title", message = text <- "MSG")
  msg <- try_consume(q)

  ## now we simulate a crash, so the connection embedded in `msg` is closed
  rm(msg)
  gc()

  ## it is requeued 
  msg <- try_consume(q)
  expect_false(is.null(msg))
  if (!is.null(msg)) {
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }

  ## fail it again
  rm(msg)
  gc()
  
  ## it is requeued again
  msg <- try_consume(q)
  expect_false(is.null(msg))
  if (!is.null(msg)) {
    expect_equal(msg$title, title)
    expect_equal(msg$message, text)
  }

  ## fail it again
  rm(msg)
  gc()

  ## not requeued any more
  msg <- try_consume(q)
  expect_null(msg)
  fail <- list_failed_messages(q)
  expect_equal(fail$title, title)
})
