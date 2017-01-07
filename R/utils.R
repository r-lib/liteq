
random_queue_name <- function() {
  paste(
    c("q", sample(c(letters, 0:9), 10, replace = TRUE)),
    collapse = ""
  )
}

`%||%` <- function(l, r) if (is.null(l)) r else l

try_silent <- function(x) {
  try(x, silent = TRUE)
}
