
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
  expect_true(pc$is_alive())

  pc$kill()
  outp <- c(outp, pp$read_all_output())
  outc <- c(outc, pc$read_all_output())

  outp <- paste(outp, collapse = "")
  outc <- paste(outc, collapse = "")

  expect_true(grepl("^O+$", outp))
  expect_true(grepl("^X+$", outc))

  expect_null(pp$get_result())
})
