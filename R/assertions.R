
#' @importFrom assertthat on_failure<- assert_that

is_string <- function(x) {
  is.character(x) &&
  length(x) == 1 &&
  !is.na(x)
}

on_failure(is_string) <- function(call, env) {
  paste0(deparse(call$x), " is not a string (length 1 character)")
}

is_crash_strategy <- function(x) {
  identical(x, "fail") || identical(x, "requeue") || is_count(x)
}

on_failure(is_crash_strategy) <- function(call, env) {
  paste0(
    deparse(call$x),
    " must be 'fail', 'requeue' or a non-negative scalar"
  )
}

is_count <- function(x) {
  is.numeric(x) && length(x) == 1 && !is.na(x) && round(x) == x
}

on_failure(is_count) <- function(call, env) {
  paste0(deparse(call$x), " is not a count (length 1 integer)")
}

is_string_or_null <- function(x) {
  is.null(x) || is_string(x)
}

on_failure(is_string_or_null) <- function(call, env) {
  paste0(deparse(call$x), " must be a string or NULL")
}

is_path <- function(x) {
  is_string(x)
}

on_failure(is_path) <- function(call, env) {
  paste0(deparse(call$x), " must be a file name")
}

is_queue <- function(x) {
  inherits(x, "liteq_queue")
}

on_failure(is_queue) <- function(call, env) {
  paste0(deparse(call$x), " must be a 'liteq_queue' object")
}

is_flag <- function(x) {
  is.logical(x) &&
  length(x) == 1 &&
  !is.na(x)
}

on_failure(is_flag) <- function(call, env) {
  paste0(deparse(call$x), " must be a flag (length 1 logical)")
}

is_message <- function(x) {
  inherits(x, "liteq_message")
}

on_failure(is_message) <- function(call, env) {
  paste0(deparse(call$x), " must be a 'liteq_message' object")
}

is_message_ids_or_null <- function(x) {
  is.null(x) ||
    (is.numeric(x) && !any(is.na(x)) && all(round(x) == x))
}

on_failure(is_message_ids_or_null) <- function(call, env) {
  paste0(deparse(call$x), " must be a vector of message ids or NULL")
}
