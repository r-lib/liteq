
context("db")

test_that("default_db", {
  db <- default_db()
  expect_true(is.character(db) && length(db) == 1)
})

test_that("ensure_db", {
  db <- tempfile()
  on.exit(unlink(db))
  expect_silent(ensure_db(db))
  expect_silent(ensure_db(db))
  expect_true(file.exists(db))
})

test_that("db_query", {
  db <- tempfile()
  on.exit(unlink(db))
  ensure_db(db)
  con <- dbConnect(SQLite(), db, synchronous = NULL)
  on.exit(dbDisconnect(con))
  db_execute(
    con, 'INSERT INTO ?table (name) VALUES (?value)',
    table = "meta", value = "foobar"
  )
  expect_equal(
    db_query(con, "SELECT name FROM meta"),
    data.frame(name = "foobar", stringsAsFactors = FALSE)
  )
})

test_that("db_execute", {
  db <- tempfile()
  on.exit(unlink(db))
  ensure_db(db)
  con <- dbConnect(SQLite(), db, synchronous = NULL)
  on.exit(dbDisconnect(con))
  db_execute(
    con, 'INSERT INTO ?table (name) VALUES (?value)',
    table = "meta", value = "foobar"
  )
  expect_equal(
    db_execute(con, "DELETE FROM meta WHERE name = ?n", n = "foobar"),
    1
  )
  expect_equal(
    db_execute(con, "DELETE FROM meta WHERE name = ?n", n = "foobar"),
    0
  )
})

test_that("do_db", {
  db <- tempfile()
  on.exit(unlink(db))
  ensure_db(db)
  do_db_write(
    db, 'INSERT INTO ?table (name) VALUES (?value)',
    table = "meta", value = "foobar"
  )
  expect_equal(
    do_db_read(db, "SELECT name FROM meta"),
    data.frame(name = "foobar", stringsAsFactors = FALSE)
  )
})

test_that("db_create_db", {
  ## Tested through ensure_db() already
})

test_that("db_create_queue", {
  ## Tested through higher level functions
})

test_that("db_list_queues", {
  ## Tested through higher level functions
})

test_that("db_publish", {
  ## Tested through higher level functions
})

test_that("db_try_consume", {
  ## Tested through higher level functions
})

test_that("db_clean_crashed", {
  ## Tested through higher level functions
})

test_that("db_consume", {
  ## Tested through higher level functions
})

test_that("db_ack", {
  ## Tested through higher level functions
})
