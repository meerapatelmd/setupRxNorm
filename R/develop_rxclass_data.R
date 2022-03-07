#' @title
#' Develop RxClass Data for a Release
#' @description
#' Collect, load, and extract RxClass API responses
#' into CONCEPT, CONCEPT_ANCESTOR, and CONCEPT_RELATIONSHIP csvs
#' as part of package installation directory.
#' @return
#' A folder at `inst/RxClass API/{version}/omop/{timestamp}` containing
#' a README with csvs.
#' @details
#' ATC classTypes and relaSources are excluded by default, but may be
#' included if desired by adding both 'ATC1-4' to `class_types` and
#' 'ATC' to `rela_sources`.
#'
#' All API responses are cached using both the RxClass Version and API Version
#' as the key. Depending on how much of the API responses are cached, this
#' script can take multiple days if not weeks.
#'
#' Since the output folder is a timestamp folder, many duplicate versions
#' can be made each time it is run. There is no QA method that will crosscheck
#' a previous version against the current version.
#'
#' @rdname develop_rxclass_data
#' @export
#' @importFrom tibble tribble
#' @importFrom dplyr filter bind_rows select mutate distinct
#' @importFrom cli cat_rule style_bold col_red cli_abort
#' @importFrom huxtable hux theme_article print_screen
#' @importFrom readr read_csv cols write_csv
#' @importFrom purrr set_names
#' @importFrom xfun sans_ext


develop_rxclass_data <-
  function(rela_sources =
             c(
               'DAILYMED',
               'MESH',
               'FDASPL',
               'FMTSME',
               'VA',
               'MEDRT',
               'RXNORM',
               'SNOMEDCT'
             ),
           class_types =
             c(
               "MESHPA",
               "EPC",
               "MOA",
               "PE",
               "PK",
               "TC",
               "VA",
               "DISEASE",
               "DISPOS",
               "CHEM",
               "SCHEDULE",
               "STRUCT"
               ),
           open_readme = TRUE) {


    # open_readme <- TRUE
    # class_types <-
    #   c(
    #     "MESHPA",
    #     "EPC",
    #     "MOA",
    #     "PE",
    #     "PK",
    #     "TC",
    #     "VA",
    #     "DISEASE",
    #     "DISPOS",
    #     "CHEM",
    #     "SCHEDULE",
    #     "STRUCT"
    #   )
    #
    # rela_sources <-
    #   "SNOMEDCT"


    lookup <- get_lookup()

    lookup <-
      lookup %>%
      dplyr::filter(relaSources %in% rela_sources) %>%
      dplyr::filter(classType %in% class_types)

    if (nrow(lookup)==0) {

      cli::cat_rule(cli::style_bold(cli::col_red(" * Error * ")),
                    line_col = "red")


      print(get_lookup())

      cli::cli_abort(
        c("No association between {.var rela_sources} and {.var class_types}. See lookup above for correct combinations.",
          "x" = "rela_sources: {glue::glue_collapse(glue::single_quote(rela_sources), sep = ', ', last = ', and ')}",
          "x" = "class_types : {glue::glue_collapse(glue::single_quote(class_types), sep = ', ', last = ', and ')}"),
        call = NULL,
        trace = NULL)

    } else {

      print_lookup(lookup = lookup)


    }


    version_key <- get_rxnav_api_version()

    for (zz in 1:nrow(lookup)) {

      class_type  <- lookup$classType[zz]
      rela_source <- lookup$relaSources[zz]

      extract_rxclass_members(class_types = class_type,
                              rela_sources = rela_source)
      extract_rxclass_graph(class_types = class_type)


      # Output Path
      path_vctr   <-
        c(getwd(),
          "inst",
          "RxClass API",
          version_key$version,
          "tmp",
          class_type)

      for (i in 1:length(path_vctr)) {

        dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

        if (!dir.exists(dir)) {

          dir.create(dir)
        }

      }

      tmp_concept_ancestor_csv <-
        file.path(dir, "CONCEPT_ANCESTOR.csv")


      if (!file.exists(tmp_concept_ancestor_csv)) {

      graph_concept_ancestor_csv <-
        file.path(getwd(),
                  "inst",
                  "RxClass API",
                  version_key$version,
                  "extracted",
                  "graph",
                  class_type,
                  "CONCEPT_ANCESTOR.csv")

      CONCEPT_ANCESTOR <-
        readr::read_csv(
          file = graph_concept_ancestor_csv,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )

      readr::write_csv(
        x = CONCEPT_ANCESTOR,
        file = tmp_concept_ancestor_csv
      )

      }


      tmp_concept_csv <-
        file.path(dir, "CONCEPT.csv")



      if (!file.exists(tmp_concept_csv)) {

     concept_classes_a <-
        list.files(
        file.path(getwd(),
                  "inst",
                  "RxClass API",
                  version_key$version,
                  "extracted",
                  "graph",
                  "processed",
                  class_type),
        recursive = TRUE,
        full.names = TRUE,
        pattern = "CONCEPT.csv") %>%
       purrr::map(readr::read_csv,
                  col_types = readr::cols(.default = "c"),
                  show_col_types = FALSE) %>%
       dplyr::bind_rows()

      concept_classes_b <-
        qa_rxclass_concept_classes() %>%
        dplyr::filter(class_type %in% class_type)


      concept_concepts <-
        list.files(
          file.path(getwd(),
                    "inst",
                    "RxClass API",
                    version_key$version,
                    "extracted",
                    "members",
                    "processed",
                    class_type),
          recursive = TRUE,
          full.names = TRUE,
          pattern = "CONCEPT_CONCEPTS.csv") %>%
        purrr::map(readr::read_csv,
                   col_types = readr::cols(.default = "c"),
                   show_col_types = FALSE) %>%
        dplyr::bind_rows()



      CONCEPT <-
        dplyr::bind_rows(
          concept_classes_a,
          concept_classes_b,
          concept_concepts
        )

      readr::write_csv(
        x = CONCEPT,
        file = tmp_concept_csv
      )


      }


      tmp_concept_relationship_csv <-
        file.path(dir, "CONCEPT_RELATIONSHIP.csv")


      if (!file.exists(tmp_concept_relationship_csv)) {
      cr_csvs <-
      list.files(
      file.path(getwd(),
                "inst",
                "RxClass API",
                version_key$version,
                "extracted",
                "members",
                "processed",
                class_type),
      full.names = TRUE,
      recursive = TRUE,
      pattern = "CONCEPT_RELATIONSHIP.csv"
      )



      cr <-
        cr_csvs %>%
        purrr::map(function(x)
        readr::read_csv(
          file = x,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE)) %>%
        dplyr::bind_rows() %>%
        dplyr::distinct()

      readr::write_csv(
        x = cr,
        file = tmp_concept_relationship_csv
      )

      }


      tmp_concept_synonym_csv <-
        file.path(dir, "CONCEPT_SYNONYM.csv")


      if (!file.exists(tmp_concept_synonym_csv)) {
        cs <-
          list.files(
            file.path(getwd(),
                      "inst",
                      "RxClass API",
                      version_key$version,
                      "extracted"),
            full.names = TRUE,
            recursive = TRUE,
            pattern = "CONCEPT_SYNONYM.csv|CONCEPT_SYNONYM_CONCEPTS.csv") %>%
          purrr::map(readr::read_csv,
                     col_types = readr::cols(.default = "c"),
                     show_col_types = FALSE) %>%
          dplyr::bind_rows() %>%
          dplyr::distinct()


        readr::write_csv(
          x = cs,
          file = tmp_concept_synonym_csv
        )


      }

    }




    load_map <-
      file.path(getwd(),
                "inst",
                "RxClass API",
                version_key$version,
                "tmp",
                class_types) %>%
     map(list.files, full.names = TRUE) %>%
     set_names(class_types) %>%
     map(
       function(x)
         x %>%
         set_names(toupper(xfun::sans_ext(basename(x)))) %>%
         as.list) %>%
        transpose()


    load_data <- list()
    for (i in seq_along(load_map)) {

      load_data[[i]] <-
      load_map[[i]] %>%
        purrr::map(read_csv,
            col_types = readr::cols(.default = "c"),
            show_col_types = FALSE) %>%
        purrr::set_names(names(load_map[[i]]))


    }
    names(load_data) <-
      names(load_map)

    load_data <-
      load_data %>%
      purrr::map(dplyr::bind_rows) %>%
      purrr::map(dplyr::distinct)


    output_folder    <- "omop"
    output_timestamp <- as.character(Sys.time())


    prior_output_dirs <-
    list.dirs(
      file.path(
        getwd(),
        "inst",
        "RxClass API",
        version_key$version,
        "omop"),
      recursive = FALSE,
      full.names = TRUE)

    # If there are directories present,
    # get the next version based on the
    # prior number. If this is the first
    # directory,m it will be set to 1.
    if (length(prior_output_dirs)>0) {

      prior_versions <-
      as.integer(basename(prior_output_dirs))

      most_recent_version <-
        max(prior_versions)


    } else {

      most_recent_version <- 0

    }

    this_version <-
      most_recent_version+1



    path_vctr   <-
      c(getwd(),
        "inst",
        "RxClass API",
        version_key$version,
        output_folder,
        this_version
        )

    for (i in 1:length(path_vctr)) {

      dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

      if (!dir.exists(dir)) {

        dir.create(dir)
      }

    }


    readr::write_csv(
      x = load_data$CONCEPT,
      file = file.path(dir, "CONCEPT.csv")
    )

    readr::write_csv(
      x = load_data$CONCEPT_SYNONYM,
      file = file.path(dir, "CONCEPT_SYNONYM.csv")
    )

    readr::write_csv(
      x = load_data$CONCEPT_RELATIONSHIP,
      file = file.path(dir, "CONCEPT_RELATIONSHIP.csv")
    )

    readr::write_csv(
      x = load_data$CONCEPT_ANCESTOR,
      file = file.path(dir, "CONCEPT_ANCESTOR.csv")
    )


    ## README

    readme_df <-
    tibble::tribble(
      ~classType, ~relaSources,
      "ATC1-4", "ATC",
      "CHEM", "DAILYMED",
      "CHEM", "FDASPL",
      "CHEM", "MEDRT",
      "DISEASE", "MEDRT",
      "DISPOS", "SNOMEDCT",
      "EPC", "DAILYMED",
      "EPC", "FDASPL",
      "MESHPA", "MESH",
      "MOA", "DAILYMED",
      "MOA", "FDASPL",
      "MOA", "MEDRT",
      "PE", "DAILYMED",
      "PE", "FDASPL",
      "PE", "MEDRT",
      "PK", "MEDRT",
      "SCHEDULE", "RXNORM",
      "STRUCT", "SNOMEDCT",
      "TC", "FMTSME",
      "VA", "VA") %>%
      left_join(lookup,
                by = c("classType", "relaSources"),
                keep = TRUE,
                suffix = c(".default", ".version")) %>%
      mutate_at(dplyr::vars(ends_with(".version")),
                ~ifelse(is.na(.), "", "X")) %>%
      rename(
        classType = classType.default,
        relaSources = relaSources.default,
        `version classType` = classType.version,
        `version relaSources` = relaSources.version
      )

    readme_df_as_lines <-
    capture.output(huxtable::print_screen(huxtable::hux(readme_df), colnames = FALSE))

    # Huxtable centers table in output, and this replaces it with an indent
    readme_df_as_lines <-
      stringr::str_replace_all(
        string = readme_df_as_lines,
        pattern = "(^[ ]{1,})([A-Za-z]{1,}.*?)",
        replacement = "\t\\2"
      )

    readme_fn <- file.path(dir, "README.md")
    cat(
      "RxClass (setupRxNorm R package)",
      "Sourced from RxNav's RxClass API: https://lhncbc.nlm.nih.gov/RxNav/APIs/RxClassAPIs.html",
      "patelmeeray@gmail.com",
      glue::glue("Version {this_version}"),
      glue::glue("{output_timestamp}"),
      "---",
      glue::glue("RxClass Version:\t\t {version_key$version}"),
      glue::glue("RxClass API Version: {version_key$apiVersion}"),
      "Contains: ",
      paste("\t", readme_df_as_lines, collapse = "\n"),
      sep = "\n",
      file = readme_fn,
      append = FALSE
    )

    if (open_readme) {

      file.edit(readme_fn)

    }




  }
