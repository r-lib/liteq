
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

test_that("delete_queue", {
  ## TODO
})

test_that("list_queues", {
  db <- tempfile()
  q <- create_queue("foo", db = db)
  expect_true("foo" %in% lapply(list_queues(db), "[[", "name"))
 })

test_that("make_queue", {
  ## Tested via other methods
})
