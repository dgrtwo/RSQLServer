#' @importFrom dplyr db_list_tables
#' @export
db_list_tables.SQLServerConnection <- function (con) {
  dbListTables(con)
}

#' @importFrom dplyr db_has_table
#' @export
db_has_table.SQLServerConnection <- function (con, table) {
  # Like method for MySQL, RSQLServer has no way to list temporary tables, so we
  # always NA to skip any local checks and rely on the database to throw
  # informative errors
  # See: https://github.com/imanuelcostigan/RSQLServer/issues/29
  NA
}

#' @importFrom dplyr db_query_fields ident
#' @export
db_query_fields.SQLServerConnection <- function (con, sql, ...) {
  dbListFields(con, sql, ...)
}

#' @importFrom dplyr db_save_query
#' @export
db_save_query.SQLServerConnection <- function (con, sql, name, temporary = TRUE,
  ...) {
  # http://smallbusiness.chron.com/create-table-query-results-microsoft-sql-50836.html
  if (temporary) name <- paste0("#", name)
  qry <- build_sql("SELECT * INTO ", ident(name), " FROM ",
    sql_subquery(con, sql), con = con)
  dbExecute(con, qry)
  name
}

#' @importFrom dplyr db_drop_table
#' @export
db_drop_table.SQLServerConnection <- function (con, table, force = FALSE, ...) {
  message("The 'force' argument is ignored.")
  dbRemoveTable(con, table)
}

#' @importFrom dplyr db_analyze ident build_sql
#' @export
db_analyze.SQLServerConnection <- function (con, table, ...) {
  TRUE
}

# Inherited db_create_index.DBIConnection method from dplyr

#' @importFrom dplyr db_explain
#' @export
db_explain.SQLServerConnection <- function (con, sql, ...) {
  # SET SHOWPLAN_ALL available from SQL Server 2000 on.
  # https://technet.microsoft.com/en-us/library/aa259203(v=sql.80).aspx
  # http://msdn.microsoft.com/en-us/library/ms187735.aspx
  # http://stackoverflow.com/a/7359705/1193481
  dbExecute(con, "SET SHOWPLAN_ALL ON")
  on.exit(dbExecute(con, "SET SHOWPLAN_ALL OFF"))
  res <- dbGetQuery(con, sql) %>%
    dplyr::select_("StmtId", "NodeId", "Parent", "PhysicalOp", "LogicalOp",
      "Argument", "TotalSubtreeCost")
  paste(utils::capture.output(print(res)), collapse = "\n")
}
