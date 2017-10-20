library(testthat)
library(liteq)

testme <- function() {
  cache <- tempfile()
  on.exit(unlink(cache, recursive = TRUE), add = TRUE)
  withr::with_envvar(
    c("LITEQ_CACHE_DIR" = cache),
    test_check("liteq")
  )
}
