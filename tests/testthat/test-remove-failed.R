
context("removing failed messages")

test_that("remove all failed messages", {

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

  ## Remove them
  remove_failed_messages(q)
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 0)
  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 1)
})


test_that("remove some failed messages", {

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
  publish(q, title = "title2", message ="MSG2")

  ## List failed messages
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 2)
  expect_equal(fail$title, c("title1", "title2"))
  expect_equal(fail$status, c("FAILED", "FAILED"))

  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 3)
  expect_equal(sum(msgs$status == "FAILED"), 2)

  ## Remove one
  remove_failed_messages(q, id = 1)
  fail <- list_failed_messages(q)
  expect_equal(nrow(fail), 1)
  expect_equal(fail$title, "title2")
  msgs <- list_messages(q)
  expect_equal(nrow(msgs), 2)
})
