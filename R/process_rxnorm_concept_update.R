#' @title
#' Process the RxNorm Concept Update Table
#' @rdname process_rxnorm_concept_update
#' @export
#' @importFrom pg13 send query write_table drop_table
#' @importFrom glue glue
#' @importFrom dplyr arrange filter

process_rxnorm_concept_update <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           checks = "",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {


    if (requires_processing(target_table = "rxnorm_concept_update")) {
      sql_statement <-
      "
      create schema if not exists rxrel;

      DROP TABLE IF EXISTS rxrel.tmp_rxnorm_concept_update;
      CREATE TABLE rxrel.tmp_rxnorm_concept_update AS (
        select distinct
        COALESCE(r.rxcui, arc.rxcui) AS rxcui_lookup,
        COALESCE(r.sab, arc.sab) AS rxcui_sab_lookup,
        COALESCE(r.tty, arc.tty) AS rxcui_tty_lookup,
        COALESCE(r.str, arc.str) AS rxcui_str_lookup,
        r.rxcui AS rxnconso_rxcui,
        arc.rxcui AS rxnatomarchive_rxcui,
        arc.merged_to_rxcui,
        COALESCE(arc.merged_to_rxcui, arc.rxcui, r.rxcui) AS maps_to_rxcui,
        CASE
        WHEN r.rxcui IS NOT NULL AND arc.rxcui IS NULL THEN 'Valid'
        WHEN r.rxcui IS NULL AND arc.rxcui IS NOT NULL AND arc.rxcui = arc.merged_to_rxcui THEN 'Deprecated'
        WHEN  r.rxcui IS NOT NULL AND r.rxcui = arc.rxcui AND arc.rxcui <> arc.merged_to_rxcui THEN 'Updated'
        WHEN r.rxcui IS NULL AND arc.rxcui IS NOT NULL AND arc.rxcui <> arc.merged_to_rxcui THEN 'Updated'
        WHEN r.rxcui = arc.rxcui AND arc.rxcui = arc.merged_to_rxcui THEN 'Valid'
        ELSE 'Invalid'
        END validity
        from rxnorm.rxnconso r
        full join rxnorm.rxnatomarchive arc
        on r.rxcui = arc.rxcui
      )
      ;
      "
      pg13::send(conn = conn,
                 conn_fun = conn_fun,
                 sql_statement = sql_statement,
                 checks = checks,
                 verbose = verbose,
                 render_sql = render_sql,
                 render_only = render_only)

      sql_statement <-
        glue::glue(
          "
          DROP TABLE IF EXISTS rxrel.tmp_rxnorm_concept_update0;
          CREATE TABLE rxrel.tmp_rxnorm_concept_update0 AS (
          SELECT DISTINCT
            rxcui_lookup AS rxcui_lookup,
            maps_to_rxcui AS maps_to_rxcui,
            maps_to_rxcui AS maps_to_rxcui0,
            validity AS validity
          FROM rxrel.tmp_rxnorm_concept_update
          );
          "
        )

      pg13::send(conn = conn,
                 conn_fun = conn_fun,
                 sql_statement = sql_statement,
                 checks = checks,
                 verbose = verbose,
                 render_sql = render_sql,
                 render_only = render_only)

    for (i in 1:10) {
    sql_statement <-
      glue::glue(
        "
        DROP TABLE IF EXISTS rxrel.tmp_rxnorm_concept_update{i}_b;
        CREATE TABLE rxrel.tmp_rxnorm_concept_update{i}_b AS (
        SELECT DISTINCT
          '{i}' AS level_of_separation,
          a.maps_to_rxcui{i-1},
          b.validity     AS validity,
          b.maps_to_rxcui AS maps_to_rxcui{i}
        FROM rxrel.tmp_rxnorm_concept_update{i-1} a
        LEFT JOIN rxrel.tmp_rxnorm_concept_update0 b
        ON a.maps_to_rxcui{i-1} = b.rxcui_lookup
        WHERE a.validity = 'Updated' AND b.validity = 'Updated'
        );

        DROP TABLE IF EXISTS rxrel.tmp_rxnorm_concept_update{i};
        CREATE TABLE rxrel.tmp_rxnorm_concept_update{i} AS (
            SELECT *
            FROM rxrel.tmp_rxnorm_concept_update{i}_b b
            WHERE b.maps_to_rxcui{i-1} NOT IN (SELECT DISTINCT maps_to_rxcui{i} FROM rxrel.tmp_rxnorm_concept_update{i}_b)

        );

        DROP TABLE rxrel.tmp_rxnorm_concept_update{i}_b;
        "
      )

    pg13::send(conn = conn,
               conn_fun = conn_fun,
               sql_statement = sql_statement,
               checks = checks,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only)

    row_count <-
      pg13::query(conn = conn,
                  conn_fun = conn_fun,
                  sql_statement = glue::glue("SELECT COUNT(*) FROM rxrel.tmp_rxnorm_concept_update{i};"),
                  checks = checks,
                  verbose = verbose,
                  render_sql = render_sql,
                  render_only = render_only) %>%
      unlist() %>%
      unname()

    if (row_count == 0) {
      pg13::send(conn = conn,
                 conn_fun = conn_fun,
                 sql_statement = glue::glue("DROP TABLE rxrel.tmp_rxnorm_concept_update{i};"),
                 checks = checks,
                 verbose = verbose,
                 render_sql = render_sql,
                 render_only = render_only)

      final_tables <- sprintf("tmp_rxnorm_concept_update%s", 1:(i-1))

      output <-
        vector(mode = "list",
               length = length(final_tables))
      names(output) <- final_tables

      for (final_table in final_tables) {
        output[[final_table]] <-
          pg13::query(
            conn = conn,
            conn_fun = conn_fun,
            sql_statement = glue::glue("SELECT * FROM rxrel.{final_table};"),
            checks = checks,
            verbose = verbose,
            render_sql = render_sql,
            render_only = render_only
          )


      }


      output <-
        output %>%
        purrr::map(dplyr::select, -level_of_separation) %>%
        purrr::reduce(dplyr::left_join) %>%
        dplyr::distinct()

      final_a <-
      output %>%
        tidyr::pivot_longer(cols = matches("[1-9]{1,}$"),
                     names_to = "level_of_separation",
                     names_prefix = "maps_to_rxcui",
                     values_to = "maps_to_rxcui",
                     values_drop_na = TRUE)

      final_b <-
      final_a %>%
        dplyr::group_by(maps_to_rxcui0) %>%
        dplyr::arrange(desc(level_of_separation),
                       .by_group = TRUE) %>%
        dplyr::filter(row_number() == 1) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(desc(level_of_separation))


      pg13::write_table(conn_fun = conn_fun,
                        schema = "rxrel",
                        table_name = "rxnorm_updated_path",
                        data = final_a,
                        drop_existing = TRUE)

      pg13::write_table(conn_fun = conn_fun,
                        schema = "rxrel",
                        table_name = "rxnorm_updated",
                        data = final_b,
                        drop_existing = TRUE)




      for (final_table in final_tables) {

        pg13::drop_table(conn_fun = conn_fun,
                         schema = "rxrel",
                         table = final_table)

      }
      break
    }


    }


    sql_statement <-
    "
    DROP TABLE IF EXISTS rxrel.rxnorm_concept_update2;
    create table rxrel.rxnorm_concept_update2 AS (
      select
      l.rxcui_lookup,
      l.rxcui_sab_lookup,
      l.rxcui_tty_lookup,
      l.rxcui_str_lookup,
      l.rxnconso_rxcui,
      l.rxnatomarchive_rxcui,
      l.merged_to_rxcui,
      COALESCE(u.maps_to_rxcui, l.maps_to_rxcui) AS maps_to_rxcui,
      COALESCE(u.validity, l.validity) AS validity,
      u.level_of_separation
      from rxrel.tmp_rxnorm_concept_update l
      left join rxrel.rxnorm_updated u
      ON u.maps_to_rxcui0 = l.maps_to_rxcui
    )
    ;
    "

    pg13::send(sql_statement = sql_statement,
               conn = conn,
               conn_fun = conn_fun,
               checks = checks,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only)


    sql_statement <-
    "
    DROP TABLE IF EXISTS rxrel.rxnorm_concept_update;
    CREATE TABLE rxrel.rxnorm_concept_update AS (
    SELECT DISTINCT
      l.*,
      r.rxcui_str_lookup AS maps_to_str,
      r.rxcui_sab_lookup AS maps_to_sab,
      r.rxcui_tty_lookup AS maps_to_tty
    FROM rxrel.rxnorm_concept_update2 l
    LEFT JOIN rxrel.rxnorm_concept_update2 r
    ON r.rxcui_lookup = l.maps_to_rxcui
    );
    "

    pg13::send(conn = conn,
               conn_fun = conn_fun,
               sql_statement = sql_statement,
               checks = checks,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only)


    pg13::drop_table(conn_fun = conn_fun,
                     schema = "rxrel",
                     table = "rxnorm_concept_update2")

    pg13::drop_table(conn_fun = conn_fun,
                     schema = "rxrel",
                     table = "tmp_rxnorm_concept_update0")

    pg13::drop_table(conn_fun = conn_fun,
                     schema = "rxrel",
                     table = "tmp_rxnorm_concept_update")

    log_processing(target_table = "rxnorm_updated")
    log_processing(target_table = "rxnorm_updated_path")
    log_processing(target_table = "rxnorm_concept_update")


    }

  }
