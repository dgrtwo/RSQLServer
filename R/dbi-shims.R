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

#' @importFrom dplyr db_create_table escape ident sql_vector build_sql
#' @export

db_create_table.SQLServerConnection <- function(con, table, types,
  temporary = FALSE, ...) {
  assertthat::assert_that(assertthat::is.string(table), is.character(types))
  field_names <- escape(ident(names(types)), collapse = NULL, con = con)
  fields <- sql_vector(paste0(field_names, " ", types), parens = TRUE,
    collapse = ", ", con = con)
  if (temporary) table <- paste0("#", table)
  sql <- build_sql("CREATE TABLE ", ident(table), " ", fields, con = con)
  dbExecute(con, sql)
  # Needs to return table name as temp tables are prefixed by `#` in SQL Server
  table
}

#' @importFrom dplyr db_insert_into
#' @export

db_insert_into.SQLServerConnection <- function(con, table, values, ...) {
  dbWriteTable(con, table, values, append = TRUE)
}

#' @importFrom dplyr db_drop_table
#' @export

db_drop_table.SQLServerConnection <- function(con, table, force = FALSE, ...) {
  # IF EXISTS only supported by SQL Server 2016 (v. 13) and above.
  qry <- paste0("DROP TABLE ", if (force && con$db.version > 12) "IF EXISTS ",
    dbQuoteIdentifier(con, table))
  assertthat::is.count(dbExecute(con, qry))
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
