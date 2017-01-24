
context("requeueing failed messages")

test_that("requeue all failed messages", {

  ## Create queue
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)

  ## Fail some messages
  publish(q, title = "title1", message ="MSG1")
  msg <- try_consume(q)
  nack(msg)

  publish(q, title = "title2", message ="MSG2")
  msg <- try_consume(q)
  nack(msg)

  ## And add another one
  publish(q, title = "title3", message ="MSG3")

  ## List failed messages
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 2)
  expect_equal(fail$title, c("title1", "title2"))
  expect_equal(fail$status, c("FAILED", "FAILED"))

  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 3)
  expect_equal(sum(msgs$status == "FAILED"), 2)

  ## Requeue them
  requeue_failed_messages(q)
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 0)
  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 3)
  expect_equal(sort(msgs$title), sort(c("title1", "title2", "title3")))
})

test_that("requeue some failed messages", {

  ## Create queue
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)

  ## Fail some messages
  publish(q, title = "title1", message ="MSG1")
  msg <- try_consume(q)
  nack(msg)

  publish(q, title = "title2", message ="MSG2")
  msg <- try_consume(q)
  nack(msg)

  ## And add another one
  publish(q, title = "title3", message ="MSG3")

  ## List failed messages
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 2)
  expect_equal(fail$title, c("title1", "title2"))
  expect_equal(fail$status, c("FAILED", "FAILED"))

  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 3)
  expect_equal(sum(msgs$status == "FAILED"), 2)

  ## Requeue them
  requeue_failed_messages(q, id = 1)
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 1)
  expect_equal(sort(fail$title), "title2")
  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 3)
  expect_equal(sum(msgs$status == "FAILED"), 1)
})
