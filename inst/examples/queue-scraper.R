# The aim is to showcase queuing a scraper in R and requery failed attempts
# To simulate "failed attempts" we gonna write a flawed scraper

# Setup
# a) Scrape Hackernews ticker (https://news.ycombinator.com/)
# b) Scrape the html-titles of all websites that are linked by hackernews

# The scraper to so b) is flawed

# Doublecheck that the queue knows which scrapes failed

# P.S.: This is a sequential/blocking example.
# If you really would like to scrape a website a non-blocking approach might 
# be much faster. E.g. https://github.com/jeroenooms/curl/blob/master/examples/crawler.R

library(liteq)
library(DBI)
require(tidyverse)
require(jsonlite)

# Setup Query DB ----------------------------------------------------------

queues_db <- "~/Downloads/queuesdb"
q <- ensure_queue("jobs", db = queues_db)

urls <- paste0("https://news.ycombinator.com/news?p=", 1)
map(urls, partial(publish, q = q, title = "get_links"))


# Setup result DB ---------------------------------------------------------

result_db <- src_sqlite(path = "~/Downloads/resultdb", create = TRUE)
result_tbl <- tibble(
       id = integer(),
       title = character(),
       points = integer(),
       comments = integer(),
       html_title = character(),
       url = character(),
       timestamp = integer()) %>% 
  copy_to(result_db, ., "hackernews", indexes = list(id_idx = "id")) # evtl. noch indexes setzen

# tbl <- tbl(result_db, "hackernews")


# scraper functions --------------------------------------------------------

parse_hackernews_row <- function(row){
  tibble(
    id = row[[1]] %>% html_attr("id") %>% as.integer,
    title = row[[1]] %>% html_node(".storylink") %>% html_text,
    points = row[[2]] %>% html_node(".score") %>% html_text %>% parse_number,
    comments = row[[2]] %>% html_node("a+ a") %>% html_text %>% parse_number(na = c("", "NA", "discuss")),
    html_title = character(1),
    url = row[[1]] %>% html_node(".storylink") %>% html_attr("href"),
    timestamp = as.integer(Sys.time())
  )
}

scrape_hackernews <- function(url){
  doc <- read_html(url)
  doc %>% html_node(".itemlist") %>% html_nodes("tr") %>%
    .[-c(length(.):(length(.)-1))] %>% # Exclude "more" row at the bottom
    {split(., rep(seq(length(.)/3), each = 3))} %>% # Group 3 tr together as one row
    map_df(parse_hackernews_row)
}

# This scraper fails # e.g. for JS-Framework websites link
# Angular and React if they set the html-title with JS 
flawed_scraper <- function(url){
  read_html(url) %>% html_node("title") %>% html_text
}

do_job <- function(msg, db, q){
  if(msg$title == "get_links"){
    out <- scrape_hackernews(msg$message)
    messages <- build_messages(out[, c("id", "url")])
    map(unlist(messages), partial(publish, q = q, title = "get_title"))
    dbWriteTable(db$con, "hackernews", out, append = TRUE)
  }
  if(msg$title == "get_title"){
    message <- fromJSON(msg$message)
    out <- flawed_scraper(message$url)
    sql <- sprintf("UPDATE hackernews SET html_title='%s' WHERE id=%d", out, message$id)
    dbExecute(db$con, sql)
  }
}

# liteq utils -------------------------------------------------------------

build_messages <- function(dat){
  by_row(dat, ~toJSON(as.list(.), auto_unbox = TRUE))$.out 
}


# Actual scraper ----------------------------------------------------------

msg <- try_consume(q)
while(!is.null(msg)){
  cat(msg$id, msg$title, "\n")
  tryCatch({do_job(msg, result_db, q); ack(msg)}, error = function(e) nack(msg))
  msg <- try_consume(q)
}


# Doublecheck results -----------------------------------------------------

failed_messages <- list_failed_messages(q)
result_without_html_table <- result_tbl %>% filter(html_title == "") %>% collect
nrow(failed_messages) == nrow(result_without_html_table) # TRUE
