
context("concurrency")

test_that("pressure test", {

  ## peace
  skip_on_cran()

  producer <- function() {
    library(liteq)
    q <- ensure_queue("q", db = "db.txt")
    limit <- Sys.time() + 30
    while (Sys.time() < limit) {
      publish(q, title = "title", message = "message")
      cat("O")
    }
  }

  consumer <- function() {
    library(liteq)
    q <- ensure_queue("q", db = "db.txt")
    while (TRUE) {
      msg <- consume(q)
      ack(msg)
      cat("X")
    }
  }

  dir.create(tmp <- tempfile())
  on.exit(unlink(tmp, recursive = TRUE), add = TRUE)
  withr::local_dir(tmp)

  pp <- callr::r_bg(producer)
  pc <- callr::r_bg(consumer)

  outp <- character()
  outc <- character()

  while (pp$is_alive()) {
    processx::poll(list(pp, pc), -1)
    outp <- c(outp, pp$read_output())
    outc <- c(outc, pc$read_output())
  }

  expect_equal(pp$get_exit_status(), 0)

  pc$kill(close_connections = FALSE)
  outp <- c(outp, pp$read_all_output())
  outc <- c(outc, pc$read_all_output())
  close(pc$get_output_connection())
  close(pc$get_error_connection())

  outp <- paste(outp, collapse = "")
  outc <- paste(outc, collapse = "")

  expect_true(grepl("^O+$", outp))
  expect_true(grepl("^X+$", outc))

  expect_null(pp$get_result())
})

test_that("many workers, short busy timeout (#28)", {

  skip_on_cran()

  task <- function(db) {
    library(liteq)
    q <- ensure_queue("jobs", db = db)
    msg <- consume(q, poll_interval = 100)
    Sys.sleep(3)
    ack(msg)
    msg$title
  }

  withr::local_envvar(R_LITEQ_BUSY_TIMEOUT = "1000")
  db <- tempfile()
  on.exit(unlink(db), add = TRUE)
  q <- ensure_queue("jobs", db = db)

  n <- 20
  workers <- replicate(n, callr::r_bg(task, list(db = db)), simplify = FALSE)
  on.exit(lapply(workers, function(w) w$kill()), add = TRUE)

  titles <- as.character(seq_len(n))
  publish(q, titles[1:(n / 2)], rep("x", n / 2))
  Sys.sleep(2)
  publish(q, titles[(n / 2 + 1):n], rep("x", n / 2))

  limit <- Sys.time() + 60
  while (nrow(list_messages(q)) > 0 && Sys.time() < limit) Sys.sleep(0.2)
  expect_true(is_empty(q))

  for (w in workers) w$wait(10000)
  res <- vapply(workers, function(w) w$get_result(), character(1))
  expect_equal(sort(as.integer(res)), seq_len(n))
})
