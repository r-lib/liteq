
random_id <- function(length = 10, prefix = "q") {
  paste(
    c(prefix, sample(c(letters, 0:9), length, replace = TRUE)),
    collapse = ""
  )
}

random_queue_name <- function() {
  random_id(length = 10, prefix = "q")
}

random_lock_name <- function() {
  random_id(length = 6, prefix = "")
}

`%||%` <- function(l, r) if (is.null(l)) r else l

try_silent <- function(x) {
  try(x, silent = TRUE)
}
