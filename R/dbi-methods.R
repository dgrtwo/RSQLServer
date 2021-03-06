#' @include dbi-classes.R
NULL

# Drivers ----------------------------------------------------------------

#' SQLServerDriver class and methods
#'
#' \code{SQLServer()} creates a \code{SQLServerDriver} object and is based on
#' the jTDS driver while \code{dbConnect()} provides a convenient interface to
#' connecting to a SQL Server database using this driver.
#'
#' @references
#' \href{http://jtds.sourceforge.net/doc/net/sourceforge/jtds/jdbc/Driver.html}{jTDS API doc for Driver class}
#' @examples
#' \dontrun{
#' SQLServer()
#' }
#' @rdname SQLServer
#' @export

SQLServer <- function () {
  rJava::.jaddClassPath(jtds_class_path())
  drv <- rJava::.jnew("net.sourceforge.jtds.jdbc.Driver", check = FALSE)
  rJava::.jcheck(TRUE)
  if (rJava::is.jnull(drv)) drv <- rJava::.jnull()
  new("SQLServerDriver", jdrv = drv)
}


#' @param drv An objected of class \code{\linkS4class{SQLServerDriver}}, or an
#'   existing \code{\linkS4class{SQLServerConnection}}. If a connection, the
#'   connection will be cloned.
#' @template sqlserver-parameters
#' @return \code{SQLServer()} returns an object of class
#'   \code{SQLServerDriver}; \code{dbConnect()} returns a
#'   \code{\linkS4class{SQLServerConnection}} object.
#' @examples
#' # View sql.yaml file bundled in package
#' file <- system.file("extdata", "sql.yaml", package = "RSQLServer")
#' readLines(file)
#' # Connect using ~/sql.yaml file
#' \dontrun{
#' if (have_test_server()) {
#'  dbConnect(RSQLServer::SQLServer(), "TEST")
#' }
#' # Example where ~/sql.yaml does not exist or where the server
#' # is not in the YAML file.
#' dbConnect(RSQLServer::SQLServer(), server="11.1.111.11", port=1434,
#'    properties=list(useNTLMv2="true", domain="myco", user="me",
#'      password="asecret"))
#' }
#' @rdname SQLServer
#' @export

setMethod('dbConnect', "SQLServerDriver",
  definition = function (drv, server, file = NULL, database = NULL,
    type = NULL, port = NULL, properties = NULL) {

    # Set default values for arguments
    file <- file %||% file.path(Sys.getenv("HOME"), "sql.yaml")
    database <- database %||% ""
    type <- type %||% "sqlserver"
    port <- port %||% ""
    properties <- properties %||% list()

    # Use sql.yaml file if file is not missing. If so, then the paramaters
    # type, port and connection properties will be ignored and the
    # information in sql.yaml will be used instead.
    if (file.exists(file)) {
      sd <- get_server_details(server, file)
    } else {
      sd <- NULL
    }
    # Server details must include type and port otherwise get_server_file fails
    if (!is.null(sd)) {
      server <- sd$server
      sd$server <- NULL
      type <- sd$type
      sd$type <- NULL
      port <- sd$port
      sd$port <- NULL
      properties <- sd
    }
    url <- jtds_url(server, type, port, database, properties)
    jc <- rJava::.jcall(drv@jdrv, "Ljava/sql/Connection;", "connect", url,
      rJava::.jnew('java/util/Properties'))
    new("SQLServerConnection", jc = jc)
  }
)

#' @rdname SQLServerDriver-class
#' @export

setMethod('dbGetInfo', 'SQLServerDriver', definition = function (dbObj, ...) {
  list(name = 'RSQLServer (jTDS)',
    # jTDS is a JDBC 3.0 driver. This can be determined by calling the
    # getDriverVersion() method of the JtdsDatabaseMetaData class. But
    # this method isn't defined for Driver class - so hard coded.
    driver.version = "3.0",
    client.version = rJava::.jcall(dbObj@jdrv, "S", "getVersion"),
    # Max connection defined server side rather than by driver.
    max.connections = NA)
})

#' @export
#' @rdname SQLServerDriver-class
setMethod("dbUnloadDriver", "SQLServerDriver", function(drv, ...) TRUE)

# DBI methods inherited from DBI
# dbDriver()
# show()

# Connections ------------------------------------------------------------

#' @rdname SQLServerConnection-class
#' @export

setMethod('dbGetInfo', 'SQLServerConnection',
  definition = function (dbObj, ...) {
    list(
      username = rJava::.jfield(dbObj@jc, "S", "user"),
      host = rJava::.jfield(dbObj@jc, "S", "serverName"),
      port = rJava::.jfield(dbObj@jc, "I", "portNumber"),
      dbname = rJava::.jfield(dbObj@jc, "S", "currentDatabase"),
      db.version = rJava::.jfield(dbObj@jc, "S", "databaseProductVersion")
    )
  }
)

#' @rdname SQLServerConnection-class
#' @export

setMethod('dbIsValid', 'SQLServerConnection', function (dbObj, ...) {
  !rJava::.jcall(dbObj@jc, "Z", "isClosed")
})

#' @rdname SQLServerConnection-class
#' @export

setMethod("dbDisconnect", "SQLServerConnection", function (conn, ...) {
  if (rJava::.jcall(conn@jc, "Z", "isClosed"))  {
    warning("The connection has already been closed")
    FALSE
  } else {
    rJava::.jcall(conn@jc, "V", "close")
    TRUE
  }
})

#' @rdname SQLServerConnection-class
#' @export

setMethod("dbSendQuery", c("SQLServerConnection", "character"),
  def = function (conn, statement, ...) {
    # Notes:
    # 1. Unlike RJDBC, this method does **not** support executing stored procs
    # or precompiled statements as these do not appear to be explicitly
    # supported by any of the rstats-db backends.
    # 2. This method is only responsible for sending SELECT statements which have
    # to return ResultSet objects. To execute data definition or manipulation
    # commands such as CREATE TABLE or UPDATE, use dbExecute instead.
    assertthat::assert_that(assertthat::is.string(statement))
    stat <- rJava::.jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
    jdbc_exception(stat, "Unable to create simple JDBC statement ", statement)
    jr <- rJava::.jcall(stat, "Ljava/sql/ResultSet;", "executeQuery",
      statement, check = FALSE)
    jdbc_exception(jr, "Unable to retrieve JDBC result set for ", statement)
    md <- rJava::.jcall(jr, "Ljava/sql/ResultSetMetaData;", "getMetaData",
      check = FALSE)
    jdbc_exception(md, "Unable to retrieve JDBC result set meta data for ",
      statement, " in dbSendQuery")
    new("SQLServerResult", jr = jr, md = md, stat = stat, pull = rJava::.jnull())
})

#' @rdname dbExecute
#' @export

setMethod("dbExecute", c("SQLServerConnection", "character"),
  def = function (conn, statement, ..., list = NULL) {
    # Modified from RJDBC
    # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L108
    # See comments to dbSendQuery. dbExecute doesn't support calling stored
    # procedures that do not return results.
    assertthat::assert_that(assertthat::is.string(statement))
    if (length(list(...)) || length(list)) {
      stat <- rJava::.jcall(conn@jc, "Ljava/sql/PreparedStatement;",
        "prepareStatement", statement, check = FALSE)
      jdbc_exception(stat, "Unable to execute JDBC prepared statement ",
        statement)
      # this will fix issue #4 and http://stackoverflow.com/q/21603660/2161065
      on.exit(rJava::.jcall(stat, "V", "close"))
      if (length(list(...))) {
        .fillStatementParameters(stat, list(...))
      }
      if (!is.null(list)) {
        .fillStatementParameters(stat, list)
      }
      res <- rJava::.jcall(stat, "I", "executeUpdate", check = FALSE)
    } else {
      stat <- rJava::.jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
      jdbc_exception(stat, "Unable to create JDBC statement ", statement)
      # In theory following is not necesary since 'stat' will go away and be
      # collected, but apparently it may be too late for Oracle (ORA-01000)
      on.exit(rJava::.jcall(stat, "V", "close"))
      res <- rJava::.jcall(stat, "I", "executeUpdate", statement, check = FALSE)
    }
    x <- rJava::.jgetEx(TRUE)
    if (!rJava::is.jnull(x)) {
      stop("execute JDBC update query failed in dbExecute (",
        rJava::.jcall(x, "S", "getMessage"), ")")
    } else {
      is.integer(res)
    }
  }
)

#' @rdname dbExecute
#' @export
dbSendUpdate <- function (conn, statement, ...) {
  .Deprecated("dbExecute")
  dbExecute(conn, statement)
}

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbReadTable", c("SQLServerConnection", "character"),
  function(conn, name, ...) {
    sql <- paste("SELECT * FROM", dbQuoteIdentifier(conn, name))
    dbGetQuery(conn, sql)
})

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbDataType", c("SQLServerConnection", "ANY"),
  def = function (dbObj, obj, ...) {
    # GOING FROM R data type to SQL Server data type
    # http://jtds.sourceforge.net/typemap.html
    # https://msdn.microsoft.com/en-us/library/ms187752.aspx
    # https://msdn.microsoft.com/en-us/library/ms187752(v=sql.90).aspx

    #### Helper functions
    char_type <- function (x) {
      # SQL Server 2000 does not support nvarchar(max) type.
      # TEXT is being deprecated. Make sure SQL types are UNICODE variants
      # (prefixed by N).
      # https://technet.microsoft.com/en-us/library/aa258271(v=sql.80).aspx
      if (dbGetInfo(dbObj)$db.version < 9) {
        "NVARCHAR(4000)"
      } else {
        "NVARCHAR(MAX)"
      }
    }

    binary_type <- function (x) {
      # SQL Server 2000 does not support varbinary(max) type.
      if (dbGetInfo(dbObj)$db.version < 9) {
        # https://technet.microsoft.com/en-us/library/aa225972(v=sql.80).aspx
        "VARBINARY(8000)"
      } else {
        "VARBINARY(MAX)"
      }
    }

    date_type <- function (x) {
      if (dbGetInfo(dbObj)$db.version < 10) {
        # DATE available in >= SQL Server 2008 (>= v.10)
        "DATETIME"
      } else {
        "DATE"
      }
    }
    ####

    if (is.factor(obj)) return(char_type(obj))
    if (inherits(obj, "POSIXct")) return("DATETIME")
    if (inherits(obj, "Date")) return(date_type(obj))

    switch(typeof(obj),
      logical = "BIT",
      integer = "INT",
      double = "FLOAT",
      raw = binary_type(obj),
      character = char_type(obj),
      list = binary_type(obj),
      stop("Unsupported type", call. = FALSE)
    )
  }
)

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbListTables", "SQLServerConnection",
  function(conn, pattern = "%", ...) {
  # Modified from RJDBC:
  # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L161
  md <- rJava::.jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData",
    check = FALSE)
  jdbc_exception(md, "Unable to retrieve JDBC database metadata")
  # Create arguments for call to getTables
  jns <- rJava::.jnull("java/lang/String")
  table_types <- rJava::.jarray(c("TABLE", "VIEW"))
  rs <- rJava::.jcall(md, "Ljava/sql/ResultSet;", "getTables",
    jns, jns, pattern, table_types, check = FALSE)
  jdbc_exception(rs, "Unable to retrieve JDBC tables list")
  on.exit(rJava::.jcall(rs, "V", "close"))
  tbls <- character()
  while (rJava::.jcall(rs, "Z", "next")) {
    schema <- rJava::.jcall(rs, "S", "getString", "TABLE_SCHEM")
    sys_schemas <- c("sys", "INFORMATION_SCHEMA")
    if (!(schema %in% sys_schemas)) {
      tbls <- c(tbls, rJava::.jcall(rs, "S", "getString", "TABLE_NAME"))
    }
  }
  tbls
})

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbExistsTable", "SQLServerConnection", function (conn, name, ...) {
  length(dbListTables(conn, name)) > 0
})

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbRemoveTable", "SQLServerConnection", function (conn, name, ...) {
  res <- dbExecute(conn, paste0("DROP TABLE ", dbQuoteIdentifier(conn, name)))
  assertthat::is.count(res)
})

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbBegin", "SQLServerConnection", function (conn, ...) {
  dbExecute(conn, "BEGIN TRANSACTION")
})

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbCommit", "SQLServerConnection", function (conn, ...) {
  dbExecute(conn, "COMMIT TRANSACTION")
})

#' @rdname SQLServerConnection-class
#' @export
setMethod("dbRollback", "SQLServerConnection", function (conn, ...) {
  dbExecute(conn, "ROLLBACK TRANSACTION")
})


#' @rdname SQLServerConnection-class
#' @export
setMethod("dbWriteTable", "SQLServerConnection",
  function (conn, name, value, overwrite = TRUE, append = FALSE) {

    assertthat::assert_that(is.data.frame(value), ncol(value) > 0,
      !(overwrite && append))

    dbBegin(conn)
    on.exit(dbRollback(conn))

    name <- dbQuoteIdentifier(conn, name)
    tbl_exists <- dbExistsTable(conn, name)

    if (tbl_exists && !append && !overwrite) {
      stop("The table ", name, " exists but you are not overwriting or ",
        "appending to this table.", call. = FALSE)
    }

    if (!tbl_exists && append) {
      stop("The table ", name, " does not exist but you are trying to ",
        "append data to it.")
    }

    # NB: if table "name" does not exist, having "overwrite" set to TRUE does
    # not cause problems, so no need for error handling in this case.

    if (tbl_exists && overwrite) {
      dbRemoveTable(conn, name)
    }

    if (!tbl_exists || overwrite) {
      dbExecute(conn, sqlCreateTable(conn, name, value))
    }

    if (nrow(value) > 0) {
      fields <- dbQuoteIdentifier(conn, names(value))
      params <- rep("?", length(fields))
      sql <- paste0(
        "INSERT INTO ", name, " (", paste0(fields, collapse = ", "), ")\n",
        "VALUES (", paste0(params, collapse = ", "), ")")
      for (j in seq_along(value[[1]])) {
        dbExecute(conn, sql, list = as.list(value[j, ]))
      }
    }

    # Overwrite on.exist(dbRollback(conn)) call above as now guaranteed
    # commit success
    on.exit(NULL)
    dbCommit(conn)
    TRUE
})

setMethod("dbListFields", "SQLServerConnection",
  function(conn, name, pattern = "%", ...) {
    # Based on RJDBC
    # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L190
    md <- rJava::.jcall(conn@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData",
      check = FALSE)
    jdbc_exception(md, "Unable to retrieve database metadata")
    # Create arguments for call to getTables
    jns <- rJava::.jnull("java/lang/String")
    rs <- rJava::.jcall(md, "Ljava/sql/ResultSet;", "getColumns",
      jns, jns, name, pattern, check = FALSE)
    jdbc_exception(rs, "Unable to retrieve column names")
    on.exit(rJava::.jcall(rs, "V", "close"))
    cns <- character()
    while (rJava::.jcall(rs, "Z", "next")) {
      cns <- c(cns, rJava::.jcall(rs, "S", "getString", "COLUMN_NAME"))
    }
    cns
})

setMethod("dbListResults", "SQLServerConnection", function(conn, ...) {
  # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L150
  warning("JDBC does not maintain a list of active results.")
  NULL
})

setMethod("dbGetException", "SQLServerConnection", def = function(conn, ...) {
  # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L143
  # Function to be deprecated from DBI: https://github.com/rstats-db/DBI/issues/51
  list()
})


# Inherited from DBI:
# show()
# dbQuoteString()
# dbQuoteIdentifier()


# Results ----------------------------------------------------------------

#' @rdname SQLServerResult-class
#' @export
setMethod ('dbIsValid', 'SQLServerResult', function (dbObj) {
  !rJava::.jcall(dbObj@jr, "Z", "isClosed")
})

#' @rdname SQLServerResult-class
#' @export
setMethod("dbFetch", c("SQLServerResult", "numeric"),
  def = function (res, n, ...) {
    fetch(res, n, ...)
})

#' @rdname SQLServerResult-class
#' @export
setMethod("fetch", c("SQLServerResult", "numeric"),
  function(res, n = -1, block = 2048L, ...) {
    # Based on:
    # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L287
    assertthat::assert_that(assertthat::is.count(block))

    ###### Initialise JVM side cache of results
    rp <- res@pull
    rJava::.jaddClassPath(pull_class_path())
    if (rJava::is.jnull(rp)) {
      rp <- rJava::.jnew("com/github/RSQLServer/MSSQLResultPull",
        rJava::.jcast(res@jr, "java/sql/ResultSet"))
      jdbc_exception(rp, "cannot instantiate MSSQLResultPull helper class")
    }

    ###### Build list that will store data and be coerced into data frame
    # Field type integers are defined in MSSQLResultPull class
    # constant ints CT_STRING, CT_NUMERIC and CT_INT where:
    # 0L - string
    # 1L - double
    # 2L - integer
    # 3L - date
    # 4L - datetime/timestamp
    cts <- rJava::.jcall(rp, "[I", "mapColumns")
    out <- vector("list", length(cts))

    ###### Fetch into cache and pull from cache into R
    if (n < 0L) { ## infinite pull
      stride <- 32768L  ## start fairly small to support tiny queries and increase later
      while ((nrec <- rJava::.jcall(rp, "I", "fetch", stride, block)) > 0L) {
        out <- fetch_rp(rp, out, cts)
        if (nrec < stride) break
        stride <- 524288L # 512k
      }
    } else if (n > 0L) {
      nrec <- rJava::.jcall(rp, "I", "fetch", as.integer(n), block)
      out <- fetch_rp(rp, out, cts)
    } else { # n == 0L
      out <- fetch_rp(rp, out, cts)
    }
    names(out) <- rJava::.jcall(rp, "[S", "columnNames")
    if (length(out[[1]]) > 0) {
      out <- purrr::map_if(out, cts == 3L, as.Date,
        format = "%Y-%m-%d")
      out <- purrr::map_if(out, cts == 4L, as.POSIXct,
        tz = "UTC", format = "%Y-%m-%d %H:%M:%OS")
      out <- purrr::map_if(out, cts == 5L, as.logical)
    }
    # as.data.frame is expensive - create it on the fly from the list
    attr(out, "row.names") <- c(NA_integer_, length(out[[1]]))
    class(out) <- "data.frame"
    out
})

#' @rdname SQLServerResult-class
#' @export
setMethod("dbGetInfo", "SQLServerResult", def = function (dbObj, ...) {
  list(
    statement = dbObj@stat,
    row.count = rJava::.jcall(dbObj@jr, "I", "getRow"),
    rows.affected = rJava::.jcall(dbObj@jr, "I", "getFetchSize"),
    # http://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html#isAfterLast()
    has.completed = rJava::.jcall(dbObj@jr, "Z", "isAfterLast")
  )
})

#' @rdname SQLServerResult-class
#' @importFrom dplyr data_frame
#' @export
setMethod("dbColumnInfo", "SQLServerResult", def = function (res, ...) {
  # Inspired by RJDBC method for JDBCResult
  # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R
  cols <- rJava::.jcall(res@md, "I", "getColumnCount")
  df <- data_frame(field.name = character(),
    field.type = character(),
    data.type = character())
  if (cols < 1) return(df)
  for (i in 1:cols) {
    df$field.name[i] <- rJava::.jcall(res@md, "S", "getColumnName", i)
    df$field.type[i] <- rJava::.jcall(res@md, "S", "getColumnTypeName", i)
    ct <- rJava::.jcall(res@md, "I", "getColumnType", i)
    df$data.type[i] <- jdbcToRType(ct)
  }
  df
})

#' @rdname SQLServerResult-class
#' @export
setMethod("dbHasCompleted", "SQLServerResult", def = function (res, ...) {
  # Need to override RJDBC method as it always returns TRUE
  dbGetInfo(res)$has.completed
})

#' @rdname SQLServerResult-class
#' @export
setMethod("dbClearResult", "SQLServerResult", function (res, ...) {
  # Need to overwrite RJDBC supplied method to pass DBItest. Needs to throw
  # warning if calling this method on cleared resultset
  if (rJava::.jcall(res@jr, "Z", "isClosed")) {
    warning("ResultSet has already been cleared", call. = FALSE)
  } else {
    rJava::.jcall(res@jr, "V", "close")
  }
  if (rJava::.jcall(res@stat, "Z", "isClosed")) {
    warning("Statement has already been cleared", call. = FALSE)
  } else {
    rJava::.jcall(res@stat, "V", "close")
  }
  TRUE
})

# Inherited from DBI:
# show()
# dbGetStatement()
# dbGetRowsAffected()
# dbGetRowCount()


# Other ----------------------------------------------------------------

.fillStatementParameters <- function(s, l) {
  # Modified from RJDBC
  # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L63
  for (i in seq_along(l)) {
    v <- l[[i]]
    if (is.na(v)) { # map NAs to NULLs (courtesy of Axel Klenk)
      sqlType <- rToJdbcType(class(v))
      rJava::.jcall(s, "V", "setNull", i, as.integer(sqlType))
    } else if (is.integer(v)) {
      rJava::.jcall(s, "V", "setInt", i, v[1])
    } else if (is.numeric(v)) {
      rJava::.jcall(s, "V", "setDouble", i, as.double(v)[1])
    } else if (is.logical(v)) {
      rJava::.jcall(s, "V", "setBoolean", i, as.logical(v)[1])
    } else if (inherits(v, "Date")) {
      # as.POSIXlt sets time to midnight UTC whereas as.POSIXct sets time to
      # local timezone. The tz argument is ignored when a Date is passed to
      # either function
      milliseconds <- as.numeric(as.POSIXlt(v)[1]) * 1000
      vdate <- rJava::.jnew("java/sql/Date", rJava::.jlong(milliseconds))
      rJava::.jcall(s, "V", "setDate", i, vdate)
    } else if (inherits(v, "POSIXct")) {
      # as.integer converts POSIXct to seconds since epoch. Timestamp
      # constructor needs milliseconds so multiply by 1000
      # http://docs.oracle.com/javase/7/docs/api/java/sql/Timestamp.html
      milliseconds <- as.numeric(v)[1] * 1000
      vtimestamp <- rJava::.jnew("java/sql/Timestamp",
        rJava::.jlong(milliseconds))
      rJava::.jcall(s, "V", "setTimestamp", i, vtimestamp)
    } else if (is.raw(v)) {
      rJava::.jcall(s, "V", "setByte", i, rJava::.jbyte(as.raw(v)[1]))
    } else {
      rJava::.jcall(s, "V", "setString", i, as.character(v)[1])
    }
  }
}

jdbc_exception <- function (object, ...) {
  # Based on RJDBC .verify.JDBC.result()
  # https://github.com/s-u/RJDBC/blob/1b7ccd4677ea49a93d909d476acf34330275b9ad/R/class.R#L18
  if (rJava::is.jnull(object)) {
    x <- rJava::.jgetEx(TRUE)
    if (rJava::is.jnull(x))
      stop(...)
    else
      stop(..., " (", rJava::.jcall(x, "S", "getMessage"), ")")
  }
}
