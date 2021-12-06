#' @title
#' Get RxNorm Version
#' @rdname get_rxnorm_version
#' @export
#' @importFrom pg13 query


get_rxnorm_version <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           checks = c("conn_status", "conn_type", "rows", "source_rows"),
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {
    pg13::query(
      conn = conn,
      conn_fun = conn_fun,
      checks = checks,
      sql_statement = "SELECT sr_release_date FROM public.setup_rxnorm_log WHERE sr_datetime IN (SELECT MAX(sr_datetime) FROM public.setup_rxnorm_log);",
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only) %>%
      unlist() %>%
      unname()
  }

#' @title
#' Check If A Table Requires Processing
#' @rdname requires_processing
#' @export
#' @importFrom pg13 query
#' @importFrom glue glue


requires_processing <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           checks = "",
           target_table,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {


    stopifnot(!missing(target_table))

    rxn_version <-
      get_rxnorm_version(conn = conn,
                         conn_fun = conn_fun,
                         checks = checks,
                         verbose = verbose,
                         render_sql = render_sql,
                         render_only = render_only)
    out <-
    pg13::query(
      conn = conn,
      conn_fun = conn_fun,
      checks = checks,
      sql_statement =
    glue::glue(
      "
      SELECT *
      FROM public.setup_rxrel_log
      WHERE
        target_table = '{target_table}' AND
        rxn_version = '{rxn_version}';"),
    verbose = verbose,
    render_sql = render_sql,
    render_only = render_only)

    nrow(out) == 0
  }


#' @title
#' Log Processing of a Table
#' @rdname requires_processing
#' @export
#' @importFrom pg13 query send
#' @importFrom glue glue

log_processing <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           checks = "",
           target_table,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {

    stopifnot(!missing(target_table))

    rxn_version <-
      get_rxnorm_version(conn = conn,
                         conn_fun = conn_fun,
                         checks = checks,
                         verbose = verbose,
                         render_sql = render_sql,
                         render_only = render_only)

    target_table_rows <-
    pg13::query(
      conn = conn,
      conn_fun = conn_fun,
      checks = checks,
      sql_statement =
        glue::glue(
          "
            SELECT COUNT(*)
            FROM rxrel.{target_table};
          "),
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only) %>%
      unlist() %>%
      unname()

    pg13::send(
      conn = conn,
      conn_fun = conn_fun,
      sql_statement =
    glue::glue(
      "
      INSERT INTO public.setup_rxrel_log
      VALUES('{Sys.time()}', '{rxn_version}', '{target_table}', '{target_table_rows}');"),
    checks = checks,
    verbose = verbose,
    render_sql = render_sql,
    render_only = render_only)

  }
