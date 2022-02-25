#' @export

setup_rxnav_rxclass <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "rxclass",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = c("conn_status", "conn_type"),
           log_schema = "public",
           log_table_name = "setup_rxnav_rxclass_log") {


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(pg13::dc(conn = conn),
              add = TRUE,
              after = TRUE
      )
    }


    inst_rxclass_data_path <-
      system.file(
        package = "setupRxNorm",
        "RxClass Data"
      )

    folder_path <-
    list.dirs(inst_rxclass_data_path,
              recursive = FALSE,
              full.names = TRUE)
    readme_path <-
      file.path(folder_path,
                "README")
    readme <-
      readLines(readme_path)

    # Timestamp folder for the omop outputs
    version_timestamp <-
      basename(folder_path)

    # Taking Contents from README
    # to write to Log
    rxclass_version <-
      grep(pattern = "RxClass Version:",
           readme,
           value = TRUE) %>%
      stringr::str_replace(
        pattern = "(RxClass Version:)(.*?)",
        replacement = "\\2") %>%
      trimws(which = "both")

    rxclass_api_version <-
      grep(pattern = "RxClass API Version:",
           readme,
           value = TRUE) %>%
      stringr::str_replace(
        pattern = "(RxClass API Version:)(.*?)",
        replacement = "\\2") %>%
      trimws(which = "both")

    # Lookup table is reconstituted from README
    # from the line after "Contains:" onward
    contains_idx <-
      grep(pattern = "Contains:",
           readme)
    start_idx <- contains_idx+1
    end_idx <- length(readme)
    lookup_lines <-
      readme[start_idx:end_idx]
    lookup_lines <-
      trimws(lookup_lines)
    tmp_tsv <-
      tempfile(fileext = ".tsv")

    cat(lookup_lines,
        file = tmp_tsv,
        sep = "\n")

    lookup <-
    readr::read_tsv(
      tmp_tsv,
      col_types = readr::cols(.default = "c"),
      show_col_types = FALSE)
    unlink(tmp_tsv)

    # DB friendly field names
    # colnames(lookup) <-
    #   c("class_type",
    #     "rela_sources",
    #     "version_class_type",
    #     "version_rela_sources")

    lookup <-
      lookup %>%
      separate(col = 1,
               into = c("class_type",
                        "rela_sources",
                        "version_class_type",
                        "version_rela_sources"),
               sep = "[ ]{1,}") %>%
      # Replace "NA" with blank space
      dplyr::mutate_at(vars(3,4), ~dplyr::if_else(is.na(.), " ", "X"))

    cli::cat_rule("Lookup")
    lookup %>%
      huxtable::hux() %>%
      huxtable::theme_article() %>%
      huxtable::print_screen(colnames = FALSE)

    lookup_csv <-
      tempfile(fileext = ".csv")
    readr::write_csv(
      x = lookup,
      file = lookup_csv,
      quote = "all",
      escape = "backslash"
    )
    on.exit(unlink(lookup_csv),
            add = TRUE,
            after = TRUE)


    fn_map <-
      list(
        CONCEPT_ANCESTOR =
          file.path(folder_path, "CONCEPT_ANCESTOR.csv"),
        CONCEPT_RELATIONSHIP =
          file.path(folder_path, "CONCEPT_RELATIONSHIP.csv"),
        CONCEPT =
          file.path(folder_path, "CONCEPT.csv")
      )

    concept_csv <- fn_map$CONCEPT
    concept_relationship_csv <- fn_map$CONCEPT_RELATIONSHIP
    concept_ancestor_csv <- fn_map$CONCEPT_ANCESTOR


    sql_statement <-
    glue::glue(
    readLines(system.file(package = "setupRxNorm",
                          "RxClass Data",
                          "load.sql")) %>%
      paste(collapse = "\n"))

    cat(sql_statement)

    pg13::send(conn = conn,
               sql_statement = sql_statement,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only,
               checks = checks)


  }
