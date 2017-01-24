
context("queue")

test_that("create_queue", {
  db <- tempfile()
  q <- create_queue("foo", db = db)
  expect_true(inherits(q, "liteq_queue"))
  expect_equal(db_list_queues(db)$name, "foo")

  ## Random name works as well
  q <- create_queue(db = db)
  expect_true(q$name %in% db_list_queues(db)$name)
})

test_that("ensure_queue", {
  db <- tempfile()
  q <- ensure_queue("foo", db = db)
  expect_true(inherits(q, "liteq_queue"))
  expect_equal(db_list_queues(db)$name, "foo")

  expect_silent(q <- ensure_queue("foo", db = db))
  expect_true(inherits(q, "liteq_queue"))
  expect_equal(db_list_queues(db)$name, "foo")
})

test_that("delete_queue", {
  db <- tempfile()
  q <- create_queue("foo", db = db)

  delete_queue(q)
  expect_false(q$name %in% db_list_queues(db)$name)

  q <- create_queue("foo", db = db)
  publish(q, title = "title", message = "")
  expect_error(delete_queue(q), "Unwilling to delete non-empty queue")
  expect_true(q$name %in% db_list_queues(db)$name)
  expect_silent(delete_queue(q, force = TRUE))
  expect_false(q$name %in% db_list_queues(db)$name)
})

test_that("list_queues", {
  db <- tempfile()
  q <- create_queue("foo", db = db)
  expect_true("foo" %in% lapply(list_queues(db), "[[", "name"))
 })

test_that("make_queue", {
  ## Tested via other methods
})
