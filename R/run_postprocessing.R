#' @title
#' Run Postprocessing Steps
#' @rdname run_postprocessing
#' @export
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc send

run_postprocessing <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           postprocess,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {
    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(pg13::dc(conn = conn),
        add = TRUE,
        after = TRUE
      )
    }

    postprocess_sql_file <-
      system.file(
        package = "setupRxNorm",
        "sql",
        "postprocessing",
        sprintf("%s.sql", postprocess)
      )

    sql_statement <-
      paste(readLines(con = postprocess_sql_file), collapse = "\n")

    pg13::send(
      conn = conn,
      sql_statement = sql_statement,
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only,
      checks = checks
    )
  }
