
context("utils")

test_that("random_queue_name", {
  x <- replicate(10, random_queue_name())
  expect_true(all(nchar(x) >= 10))
  expect_true(all(grepl("^[a-z][a-z0-9]+$", x)))
})

test_that("%||%", {
  expect_identical(NULL %||% "foo", "foo")
  expect_identical("bar" %||% "foo", "bar")
  expect_identical(NULL %||% NULL, NULL)
})

test_that("try_silent", {
  expect_silent(try_silent(stop("boo")))
  expect_silent(try_silent(1 + "A"))
  expect_output(try_silent(print("hello")), "hello")
})
