
#' Connect to Postgres Database
#'
#' @return Postgres Connection
#' @export
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect
#' @importFrom RODBC odbcDriverConnect
#' @import dplyr
#'
#' @examples
connect_postgres <- function(env_file = NULL) {
  if (!is.null(env_file)) {
    readRenviron(env_file)
  }

  conn <- odbc::dbConnect(odbc::odbc(),
                   driver = "PostgreSQL",
                   dbname = Sys.getenv("POSTGRES_DB"),
                   database = Sys.getenv("POSTGRES_DB"),
                   user = Sys.getenv("POSTGRES_USER"),
                   uid = Sys.getenv("POSTGRES_USER"),
                   password = Sys.getenv("POSTGRES_PASSWORD"),
                   pwd = Sys.getenv("POSTGRES_PASSWORD"),
                   host = Sys.getenv("POSTGRES_HOST"),
                   server = Sys.getenv("POSTGRES_HOST"),
                   port = Sys.getenv("POSTGRES_PORT"))
  conn
}

#' Title
#'
#' @param stmt
#' @param conn
#'
#' @return query result
#' @export
#'
#' @importFrom DBI dbGetQuery
#'
query <- function(stmt, conn, ...) {
  DBI::dbGetQuery(conn, stmt, ...)
}

search_db_conn <- function() {
  all_obj <- ls(envir = globalenv())
  is_db_conn <- sapply(all_obj, function(x) inherits(get(x, envir = globalenv()), c("PqConnection", "PostgreSQL")))
  if (!any(is_db_conn)) {
    return(list())
  }
  all_obj[is_db_conn]
}

#' Get DB Connection from Global Environment
#' @export
get_db_conn <- function() {
  conn <- search_db_conn()
  if (length(conn) != 1) {
    stop("Too many or no connection found")
  }
  get(conn, envir = globalenv())
}

#' Execute Query from Selection
#' @export
exec_selection <- function() {
  conn <- get_db_conn()
  sel_text <- rstudioapi::getActiveDocumentContext()$selection[[1]]$text
  name <- readline("object to want to save to: ")
  res <- query(sel_text, conn)
  if (name != "" && !is.null(name)) {
    assign(name, res, envir = globalenv())
  } else {
    print(head(res, n = 15))
  }
  message("Press Enter")
}

#' Title
#'
#' @param tbl_names
#'
#' @return
#' @export
#'
#' @examples
drop_table <- function(tbl_names) {
  lapply(tbl_names, function(x) query(sprintf("DROP TABLE %s", x), get_db_conn()))
}

#' Drop All Tables in a Postgres Schema
#'
#' @param schema_name
#'
#' @return
#' @export
#'
drop_all_tables_in_schema <- function(schema_name) {
  con <- get_db_conn()
  all_tbls <- query(paste0("SELECT * FROM pg_tables WHERE schemaname = '", schema_name, "'"), con)
  lapply(all_tbls, function(x) drop_table(paste0('"', all_tbls$schemaname, '"."', all_tbls$tablename, '"')))
}
