#' @title
#' Process the RxNorm Validity Table
#'
#' @description
#' Retrieve and cache concept batches by
#' its validity status from
#' the RxNav REST API and write a
#' table that maps an input RxCUI or RxNorm
#' code to its most current RxCUI or RxNorm code.
#'
#' @details
#'
#' Default Schema: `rxtra`
#'
#' Time Requirement: Approximately 30 minutes to an hour
#'
#' Resource Requirements:
#' \itemize{
#'   \item RxNorm schema
#'   \item RxNorm API
#' }
#'
#' @rdname process_rxnorm_validity_status
#' @export
#' @import httr
#' @import tidyverse
#' @importFrom pg13 send query write_table drop_table
#' @importFrom glue glue
#' @importFrom dplyr filter
#' @importFrom cli cli_abort

process_rxnorm_validity_status <-
  function(conn,
           conn_fun = "pg13::local_connect(verbose = {verbose})",
           processing_schema = "process_rxnorm_validity_status",
           destination_schema = "rxtra",
           rm_processing_schema = TRUE,
           checks = "",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           prior_version = NULL,
           prior_api_version = "3.1.174") {

    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    } else {

      version_key <-
        list(version = prior_version,
             apiVersion = prior_api_version)

    }

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(pg13::dc(conn = conn),
        add = TRUE,
        after = TRUE
      )
    }

    if (requires_processing(
          conn = conn,
          target_schema = destination_schema,
          target_table = "rxnorm_validity_status",
          verbose = verbose,
          render_sql = render_sql)) {


      path_vctr <-
        c(here::here(),
          "dev",
          "RxNorm API",
          version_key$version,
          "extracted",
          "status")

      for (i in 1:length(path_vctr)) {

        write_dir <-
          paste(path_vctr[1:i],
                collapse = .Platform$file.sep)


        if (!dir.exists(write_dir)) {


          dir.create(write_dir)

        }


      }


      source_file <-
        file.path(write_dir,
                  "status.csv")



      if (!file.exists(source_file)) {

        extract_rxnorm_status(
          prior_version = version_key$version,
          prior_api_version = version_key$apiVersion
        )

      }

      status_data <-
        readr::read_csv(
          file = source_file,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )


      status_version <-
        sprintf("%s %s", version_key$version, version_key$apiVersion)

      status_data <-
        status_data %>%
        dplyr::mutate(rxnorm_api_version = status_version)


      tmp_csv <- tempfile()
      readr::write_csv(
        x = status_data,
        file = tmp_csv,
        na = "",
        quote = "all"
      )


      sql_statement <-
        glue::glue(
          "
      DROP SCHEMA IF EXISTS {processing_schema} CASCADE;

      CREATE SCHEMA {processing_schema};

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status0;
      CREATE TABLE {processing_schema}.rxnorm_concept_status0 (
              rxcui     INTEGER NOT NULL,
              code      VARCHAR(50) NOT NULL,
              str       VARCHAR(3000) NOT NULL,
              tty       VARCHAR(20) NULL,
              status    VARCHAR(10) NOT NULL,
              rxnorm_api_version VARCHAR(30) NOT NULL
      )
      ;

      COPY {processing_schema}.rxnorm_concept_status0 FROM '{tmp_csv}' CSV HEADER QUOTE E'\"' NULL AS '';
      ")

      pg13::send(
        conn = conn,
        sql_statement = sql_statement,
        checks = checks,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only
      )


      sql_statement <-
        glue::glue("
      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status1;
      CREATE TABLE {processing_schema}.rxnorm_concept_status1 AS (
        SELECT
          s0.rxcui AS input_rxcui,
          s0.code AS input_code,
          s0.str AS input_str,
          s0.tty AS input_tty,
          s0.status AS input_status,
          s0.rxnorm_api_version,
          arch.merged_to_rxcui AS output_rxcui,
          rx.code AS output_code,
          rx.str  AS output_str,
          rx.tty  AS output_tty,
          CASE
    WHEN rx.tty IN ('IN', 'PIN', 'MIN') THEN 1 --Ingredient, Name from a precise ingredient, Multiple Ingredient
    WHEN rx.tty IN ('BN') THEN 2 --Fully-specified drug brand name that can not be prescribed
    WHEN rx.tty IN ('DF') THEN 3 --Dose Form
    WHEN rx.tty IN ('DFG') THEN 4 --Dose Form Group
    WHEN rx.tty IN ('SY')  THEN 5 --Synonym
      --'TMSY' --Tall Man synonym
      --'SY' --Designated synonym
      --'SCDG' --Semantic clinical drug group
      --'SCDF' --Semantic clinical drug and form
      --'SCDC' --Semantic Drug Component
      --'SCD' --Semantic Clinical Drug
      --'SBDG' --Semantic branded drug group
      --'SBDF' --Semantic branded drug and form
      --'SBDC' --Semantic Branded Drug Component
      --'SBD' --Semantic branded drug
      --'PSN' --Prescribable Names
      --'GPCK' --Generic Drug Delivery Device
      --'ET' --Entry term
      --'BPCK' --Branded Drug Delivery Device
    ELSE 6 end tty_rank
        FROM {processing_schema}.rxnorm_concept_status0 s0
        LEFT JOIN (SELECT * FROM rxnorm.rxnatomarchive WHERE sab = 'RXNORM') arch
        ON
          arch.rxcui = s0.rxcui
          AND arch.tty = s0.tty
          AND arch.str = s0.str
        LEFT JOIN (SELECT * FROM rxnorm.rxnconso WHERE sab = 'RXNORM') rx
        ON rx.rxcui = arch.merged_to_rxcui
        LEFT JOIN {processing_schema}.rxnorm_concept_status0 s1
        ON s0.rxcui = s1.rxcui
      );

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status2;
      CREATE TABLE {processing_schema}.rxnorm_concept_status2 AS (
        SELECT
         s1.*,
         ROW_NUMBER() over (partition by input_rxcui order by tty_rank) as final_rank
        FROM {processing_schema}.rxnorm_concept_status1 s1
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status3_a;
      CREATE TABLE {processing_schema}.rxnorm_concept_status3_a AS (
        SELECT DISTINCT
          s2.input_rxcui,
          s2.input_code,
          s2.input_str,
          s2.input_tty,
          s2.input_status,
          CASE
            WHEN s2.input_status = 'Remapped' THEN s2.output_rxcui
            ELSE s2.input_rxcui
            END output_rxcui,
          CASE
            WHEN s2.input_status = 'Remapped' THEN s2.output_code
            ELSE s2.input_code
            END output_code,
          CASE
            WHEN s2.input_status = 'Remapped' THEN s2.output_str
            ELSE s2.input_str
            END output_str,
          CASE
            WHEN s2.input_status = 'Remapped' THEN s2.output_tty
            ELSE s2.input_tty
            END output_tty,
          s2.tty_rank,
          s2.final_rank,
          s2.rxnorm_api_version
        FROM {processing_schema}.rxnorm_concept_status2 s2
      );

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status3_b;
      CREATE TABLE {processing_schema}.rxnorm_concept_status3_b AS (
        SELECT
         input_code,
         COUNT(DISTINCT output_code) AS output_code_cardinality,
         STRING_AGG(DISTINCT(output_code), '|') AS output_codes
        FROM {processing_schema}.rxnorm_concept_status3_a
        GROUP BY input_code
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status4;
      CREATE TABLE {processing_schema}.rxnorm_concept_status4 AS (
        SELECT
          cs3a.input_rxcui,
          cs3a.input_code,
          cs3a.input_str,
          cs3a.input_tty,
          cs3a.input_status,
          cs3a.output_rxcui,
          cs3a.output_code,
          cs3a.output_str,
          cs3a.output_tty,
          cs3b.output_code_cardinality,
          cs3b.output_codes
        FROM {processing_schema}.rxnorm_concept_status3_a cs3a
        LEFT JOIN {processing_schema}.rxnorm_concept_status3_b cs3b
        ON cs3a.input_code = cs3b.input_code
        WHERE cs3a.final_rank = 1
      )
      ;


      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status5_a_i;
      CREATE TABLE {processing_schema}.rxnorm_concept_status5_a_i AS (

        SELECT
          cs4.input_rxcui,
          cs4.input_code,
          cs4.input_str,
          cs4.input_tty,
          cs4.input_status,
          cs4b.output_rxcui,
          cs4b.output_code,
          cs4b.output_str,
          cs4b.output_tty
        FROM
          ( SELECT *
            FROM {processing_schema}.rxnorm_concept_status4
            WHERE
              output_code_cardinality = 0) cs4
        LEFT JOIN {processing_schema}.rxnorm_concept_status4 cs4b
        ON cs4.output_rxcui = cs4b.input_rxcui
      );

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status5_a_ii;
      CREATE TABLE {processing_schema}.rxnorm_concept_status5_a_ii AS (
        SELECT
          cs5ai.input_rxcui,
          COUNT(DISTINCT output_code) AS output_code_cardinality,
          STRING_AGG(DISTINCT(output_code), '|') AS output_codes
        FROM {processing_schema}.rxnorm_concept_status5_a_i cs5ai
        GROUP BY cs5ai.input_rxcui
      );

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status5_a;
      CREATE TABLE {processing_schema}.rxnorm_concept_status5_a AS (
        SELECT DISTINCT
          cs5ai.*,
          cs5aii.output_code_cardinality,
          cs5aii.output_codes
        FROM {processing_schema}.rxnorm_concept_status5_a_i cs5ai
        LEFT JOIN {processing_schema}.rxnorm_concept_status5_a_ii cs5aii
        ON cs5ai.input_rxcui = cs5aii.input_rxcui
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status5_b;
      CREATE TABLE {processing_schema}.rxnorm_concept_status5_b AS (
        SELECT *
        FROM {processing_schema}.rxnorm_concept_status4
        WHERE
         output_code_cardinality <> 0
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status6;
      CREATE TABLE {processing_schema}.rxnorm_concept_status6 AS (
         SELECT *
         FROM {processing_schema}.rxnorm_concept_status5_b
         UNION
         SELECT *
         FROM {processing_schema}.rxnorm_concept_status5_a
      )
      ;
      "
        )


      pg13::send(
        conn = conn,
        sql_statement = sql_statement,
        checks = checks,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only
      )


      # Some concepts map to a new output_rxcui, but
      # the source RxNorm RRF table does not have any info
      # regarding that RxCUI so an API call is made to get
      # the properties from RxNav

      sql_statement <-
        glue::glue(
          "
SELECT DISTINCT input_rxcui
FROM {processing_schema}.rxnorm_concept_status6
WHERE
  output_code_cardinality = 0
  AND input_rxcui IS NOT NULL
"
        )

      input_rxcuis_to_call <-
        pg13::query(
          conn = conn,
          sql_statement = sql_statement,
          checks = checks,
          verbose = verbose,
          render_sql = render_sql,
          render_only = render_only
        ) %>%
        unlist() %>%
        unname()

      path_vctr <-
        c(here::here(),
          "dev",
          "RxNorm API",
          version_key$version,
          "extracted",
          "history")

      for (i in 1:length(path_vctr)) {

        write_dir <-
          paste(path_vctr[1:i],
                collapse = .Platform$file.sep)


        if (!dir.exists(write_dir)) {


          dir.create(write_dir)

        }


      }


      source_file <-
        file.path(write_dir,
                  "history.csv")


      if (!file.exists(source_file)) {

        extract_rxcui_history(
          rxcuis = input_rxcuis_to_call,
          prior_version = version_key$version,
          prior_api_version = version_key$apiVersion
        )

      }

      rxcui_history_data <-
        readr::read_csv(
          file = source_file,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )

      tmp_csv <- tempfile()
      readr::write_csv(
        x = rxcui_history_data,
        file = tmp_csv,
        na = "",
        quote = "all"
      )


      sql_statement <-
        glue::glue(
          "
      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status7_a_i;
      CREATE TABLE {processing_schema}.rxnorm_concept_status7_a_i (
              input_rxcui     INTEGER NOT NULL,
              output_code      VARCHAR(50) NOT NULL,
              output_str       VARCHAR(3000) NOT NULL,
              output_tty       VARCHAR(20) NOT NULL,
              output_code_cardinality INTEGER NOT NULL,
              output_codes TEXT NOT NULL,
              output_source VARCHAR(20) NOT NULL,
              output_source_version VARCHAR(30) NOT NULL
      )
      ;

      COPY {processing_schema}.rxnorm_concept_status7_a_i FROM '{tmp_csv}' CSV HEADER QUOTE E'\"' NULL AS '';

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status7_a;
      CREATE TABLE {processing_schema}.rxnorm_concept_status7_a AS (
              SELECT
                cs6.input_rxcui,
                cs6.input_code,
                cs6.input_str,
                cs6.input_tty,
                cs6.input_status,
                cs7ai.output_code::integer AS output_rxcui,
                cs7ai.output_code,
                cs7ai.output_str,
                cs7ai.output_tty,
                cs7ai.output_code_cardinality,
                cs7ai.output_codes,
                cs7ai.output_source,
                cs7ai.output_source_version
              FROM {processing_schema}.rxnorm_concept_status7_a_i cs7ai
              INNER JOIN {processing_schema}.rxnorm_concept_status6 cs6
              ON cs6.input_rxcui = cs7ai.input_rxcui
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status7_b;
      CREATE TABLE {processing_schema}.rxnorm_concept_status7_b AS (
        SELECT
          cs6.*,
          'RxNorm' as output_source,
          (SELECT sr_release_date FROM public.setup_rxnorm_log WHERE sr_datetime IN (SELECT MAX(sr_datetime) FROM public.setup_rxnorm_log)) AS output_source_version
        FROM {processing_schema}.rxnorm_concept_status6 cs6
        WHERE cs6.input_rxcui NOT IN (SELECT input_rxcui FROM {processing_schema}.rxnorm_concept_status7_a)
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status8;
      CREATE TABLE {processing_schema}.rxnorm_concept_status8 AS (
        SELECT *
        FROM {processing_schema}.rxnorm_concept_status7_a
        UNION
        SELECT *
        FROM {processing_schema}.rxnorm_concept_status7_b
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status9_a;
      CREATE TABLE {processing_schema}.rxnorm_concept_status9_a AS (
        SELECT
          cs8.input_rxcui,
          cs8.input_code,
          cs8.input_str,
          cs8.input_tty,
          cs8.input_status,
          cs8.output_rxcui,
          r.code AS output_code,
          r.str  AS output_str,
          r.tty  AS output_tty,
          cs8.output_code_cardinality,
          cs8.output_codes,
          cs8.output_source,
          cs8.output_source_version
        FROM {processing_schema}.rxnorm_concept_status8 cs8
        LEFT JOIN (SELECT * FROM rxnorm.rxnconso WHERE sab = 'RXNORM')  r
        ON r.code = cs8.output_codes AND r.tty = cs8.input_tty
        WHERE
          cs8.output_rxcui IS NOT NULL
          AND cs8.output_code IS NULL
          AND cs8.output_code_cardinality = 1
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status9_b;
      CREATE TABLE {processing_schema}.rxnorm_concept_status9_b AS (
        SELECT *
        FROM {processing_schema}.rxnorm_concept_status8
        WHERE
          input_rxcui NOT IN (SELECT input_rxcui FROM {processing_schema}.rxnorm_concept_status9_a)
      )
      ;

      DROP TABLE IF EXISTS {processing_schema}.rxnorm_concept_status10;
      CREATE TABLE {processing_schema}.rxnorm_concept_status10 AS (
        SELECT *
        FROM {processing_schema}.rxnorm_concept_status9_a
        UNION
        SELECT *
        FROM {processing_schema}.rxnorm_concept_status9_b
      )
      ;
    "
        )


      pg13::send(
        conn = conn,
        sql_statement = sql_statement,
        checks = checks,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only
      )


      # Performing final export of file


      source_file <-
        system.file(
          package = "setupRxNorm",
          "RxNorm API",
          version_key$version,
          "processed",
          "rxnorm_validity_status.csv")


      if (!file.exists(source_file)) {

        path_vctr <-
          c(here::here(),
            "dev",
            "RxNorm API",
            version_key$version,
            "processed")

        for (j in 1:length(path_vctr)) {

          write_dir <-
            paste(path_vctr[1:j],
                  collapse = .Platform$file.sep)

          if (!dir.exists(write_dir)) {

            dir.create(write_dir)

          }
        }


        local_file_path <-
          file.path(write_dir, "rxnorm_validity_status.csv")


        rxnorm_validity_status_data <-
            pg13::query(
              conn = conn,
              sql_statement = glue::glue("SELECT * FROM {processing_schema}.rxnorm_concept_status10;"),
              checks = checks,
              verbose = verbose,
              render_sql = render_sql
            )

        if (!file.exists(local_file_path)) {

          readr::write_csv(
            x = rxnorm_validity_status_data,
            file = local_file_path
          )


        }




        } else {


      rxnorm_validity_status_data <-
      readr::read_csv(
        file = source_file,
        col_types = readr::cols(.default = "c"),
        show_col_types = FALSE
      )

        }

      tmp_csv <- tempfile()
      readr::write_csv(
        x = rxnorm_validity_status_data,
        file = tmp_csv,
        na = "",
        quote = "all"
      )

      sql_statement <-
        glue::glue("
        CREATE SCHEMA IF NOT EXISTS {destination_schema};
             DROP TABLE IF EXISTS {destination_schema}.rxnorm_validity_status;
             CREATE TABLE {destination_schema}.rxnorm_validity_status (
                input_rxcui integer,
                input_code character varying(50),
                input_str character varying(3000),
                input_tty character varying(20),
                input_status character varying(10),
                output_rxcui integer,
                output_code character varying(50),
                output_str character varying(3000),
                output_tty character varying(20),
                output_code_cardinality bigint,
                output_codes text,
                output_source character varying,
                output_source_version character varying
            );

        COPY {destination_schema}.rxnorm_validity_status FROM '{tmp_csv}' CSV HEADER QUOTE E'\"' NULL AS '';

        ;")

      pg13::send(
        conn = conn,
        sql_statement = sql_statement,
        checks = checks,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only
      )


      if (rm_processing_schema) {
        pg13::send(
          conn = conn,
          sql_statement = glue::glue("DROP SCHEMA {processing_schema} CASCADE;"),
          checks = checks,
          verbose = verbose,
          render_sql = render_sql,
          render_only = render_only
        )
      }

      log_processing(
        conn = conn,
        target_schema = destination_schema,
        target_table = "rxnorm_validity_status",
        verbose = verbose,
        render_sql = render_sql
      )
    }
  }
