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
#' @rdname dev_rxclass_data
#' @export
#' @importFrom tibble tribble
#' @importFrom dplyr filter bind_rows select mutate distinct
#' @importFrom cli cat_rule style_bold col_red cli_abort
#' @importFrom huxtable hux theme_article print_screen
#' @importFrom readr read_csv cols write_csv
#' @importFrom purrr set_names
#' @importFrom xfun sans_ext


dev_rxclass_data <-
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
               # "PE",
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



    lookup <-
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
      dplyr::filter(relaSources %in% rela_sources) %>%
      dplyr::filter(classType %in% class_types)

    if (nrow(lookup)==0) {

      cli::cat_rule(cli::style_bold(cli::col_red(" * Error * ")),
                    line_col = "red")

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
        huxtable::hux() %>%
        huxtable::theme_article() %>%
        huxtable::print_screen(colnames = FALSE)

      cli::cli_abort(
        c("No association between {.var rela_sources} and {.var class_types}. See lookup above for correct combinations.",
          "x" = "rela_sources: {glue::glue_collapse(glue::single_quote(rela_sources), sep = ', ', last = ', and ')}",
          "x" = "class_types : {glue::glue_collapse(glue::single_quote(class_types), sep = ', ', last = ', and ')}"),
        call = NULL,
        trace = NULL)

    } else {

      huxtable::hux(lookup) %>%
        huxtable::theme_article() %>%
        huxtable::print_screen(colnames = FALSE)


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
          "final",
          class_type)

      for (i in 1:length(path_vctr)) {

        dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

        if (!dir.exists(dir)) {

          dir.create(dir)
        }

      }

      final_concept_ancestor_csv <-
        file.path(dir, "concept_ancestor.csv")


      if (!file.exists(final_concept_ancestor_csv)) {

      concept_ancestor_staged_csv <-
        file.path(getwd(),
                  "inst",
                  "RxClass API",
                  version_key$version,
                  "staged",
                  class_type,
                  "concept_ancestor.csv")

      concept_ancestor_staged <-
        readr::read_csv(
          file = concept_ancestor_staged_csv,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )

      readr::write_csv(
        x = concept_ancestor_staged,
        file = final_concept_ancestor_csv
      )

      }


      final_concept_relationship_csv <-
        file.path(dir, "concept_relationship.csv")


      if (!file.exists(final_concept_relationship_csv)) {
      members_staged_csvs <-
      list.files(
      file.path(getwd(),
                "inst",
                "RxClass API",
                version_key$version,
                "staged",
                class_type,
                "members"),
      full.names = TRUE,
      pattern = "csv$"
      )



      members_staged <-
        members_staged_csvs %>%
        map(function(x)
        readr::read_csv(
          file = x,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE)) %>%
        purrr::set_names(xfun::sans_ext(basename(members_staged_csvs))) %>%
        dplyr::bind_rows(.id = "relaSource")

      concept_relationship_staged0 <-
        members_staged %>%
        transmute(
          concept_code_1 = classId,
          relationship_id = 'Subsumes',
          relationship_source = relaSource,
          relationship_type = Relation,
          concept_code_2_rxnorm = rxcui,
          concept_name_2_rxnorm = name,
          # concept_class_id_2_rxnorm = tty, Removed because this can always be derived elsewhere and the source does not have a tty
          # for collapsing later on
          concept_code_2_source = SourceId,
          concept_name_2_source = SourceName
        )

      # Converting map into longer format
      # Part 1: Class to Members (Source and RxNorm)
      # Inverse is not added because
      # does not serve the use case
      concept_relationship_staged1_a <-
        concept_relationship_staged0 %>%
        select(ends_with("_1"),
               starts_with("relationship_"),
               ends_with("_2_rxnorm")) %>%
        mutate(
          vocabulary_id_1 = class_type,
          vocabulary_id_2 = 'RxNorm') %>%
        rename_at(vars(ends_with("_2_rxnorm")),
                  str_remove_all, "_rxnorm") %>%
        distinct()

      concept_relationship_staged1_b <-
        concept_relationship_staged0 %>%
        select(ends_with("_1"),
               starts_with("relationship_"),
               ends_with("_2_source")) %>%
        mutate(vocabulary_id_1 = class_type,
               vocabulary_id_2 = class_type) %>%
        rename_at(vars(ends_with("_2_source")),
                  str_remove_all, "_source") %>%
        distinct()

      # Part 2: Maps to and Mapped From relationships (inverse included
      # here)
      concept_relationship_staged1_c <-
      concept_relationship_staged0 %>%
        select(ends_with("_2_rxnorm"),
               starts_with("relationship_"),
               ends_with("_2_source")) %>%
        mutate(relationship_id = "Mapped from",
               # Assumed direct mapping between the two
               relationship_type = "DIRECT") %>%
        rename_at(vars(ends_with("_2_rxnorm")),
                  str_replace_all,
                  "_2_rxnorm", "_1") %>%
        mutate(vocabulary_id_1 = "RxNorm") %>%
        rename_at(vars(ends_with("_2_source")),
                  str_replace_all,
                  "_2_source", "_2") %>%
        mutate(vocabulary_id_2 = class_type) %>%
        distinct()

      # Inverse
      concept_relationship_staged1_d <-
        concept_relationship_staged0 %>%
        select(ends_with("_2_source"),
               starts_with("relationship_"),
               ends_with("_2_rxnorm")) %>%
        mutate(relationship_id = "Maps to",
               # Assumed direct mapping between the two
               relationship_type = "DIRECT") %>%
        rename_at(vars(ends_with("_2_rxnorm")),
                  str_replace_all,
                  "_2_rxnorm", "_2") %>%
        mutate(vocabulary_id_2 = "RxNorm") %>%
        rename_at(vars(ends_with("_2_source")),
                  str_replace_all,
                  "_2_source", "_1") %>%
        mutate(vocabulary_id_1 = class_type) %>%
        distinct()


      concept_relationship_staged2 <-
        bind_rows(
          concept_relationship_staged1_a,
          concept_relationship_staged1_b,
          concept_relationship_staged1_c,
          concept_relationship_staged1_d
        )

      # Adding concept_names back if it is missing
      nodes <-
        readr::read_csv(
          file.path(getwd(),
                    "inst",
                    "RxClass API",
                    version_key$version,
                    "staged",
                    class_type,
                    "node.csv"),
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE)

      if (nrow(nodes)>0) {

        nodes <-
          nodes %>%
          transmute(concept_code_1 = classId,
                    concept_name_1 = className)

      } else {

        nodes <-
          concept_relationship_staged2 %>%
          dplyr::filter(is.na(concept_name_1)) %>%
          dplyr::distinct(concept_code_1) %>%
          dplyr::mutate(concept_name_1 = "(Missing from RxClass API)")


      }


      concept_relationship_staged3 <-
      concept_relationship_staged2 %>%
        left_join(nodes,
                  by = "concept_code_1",
                  suffix = c(".cr", ".nodes")) %>%
        mutate(concept_name_1 =
                 coalesce(concept_name_1.cr,
                          concept_name_1.nodes)) %>%
        select(!ends_with(".cr")) %>%
        select(!ends_with(".nodes")) %>%
        distinct()







      final_concept_ancestor <-
      readr::read_csv(
      final_concept_ancestor_csv,
      col_types = readr::cols(.default = "c"),
      show_col_types = FALSE)


      if (nrow(final_concept_ancestor)>0) {

      final_concept_relationship <- concept_relationship_staged3

      final_concept_csv <-
        file.path(dir, "concept.csv")

      # All Concept Ancestor concepts are classes
      concept_staged_classes <-
      bind_rows(
        final_concept_ancestor %>%
          dplyr::select(concept_code = ancestor_concept_code),
        final_concept_ancestor %>%
          dplyr::select(concept_code = descendant_concept_code)) %>%
        distinct() %>%
        # concept attributes are in the final_concept_relationship object
        inner_join(final_concept_relationship %>%
                    dplyr::mutate(concept_code = concept_code_1) %>%
                    dplyr::filter(vocabulary_id_1 == class_type,
                                  relationship_id == "Subsumes"),
                  by = "concept_code") %>%
        transmute(
          vocabulary_id = vocabulary_id_1,
          concept_code = concept_code_1,
          concept_name = concept_name_1,
          concept_class = "Class") %>%
        distinct()

      concept_staged_non_classes <-
      final_concept_relationship %>%
        dplyr::filter(vocabulary_id_1 == class_type,
                      relationship_id != "Subsumes",
                      !(concept_code_1 %in% concept_staged_classes$concept_code)) %>%
        transmute(
          vocabulary_id = vocabulary_id_1,
          concept_code = concept_code_1,
          concept_name = concept_name_1,
          concept_class = "Concept") %>%
        distinct()

      concept_staged_rxnorm  <-
        final_concept_relationship %>%
        dplyr::filter(vocabulary_id_1 == "RxNorm") %>%
        transmute(
          vocabulary_id = vocabulary_id_1,
          concept_code = concept_code_1,
          concept_name = concept_name_1,
          concept_class = "Concept") %>%
        distinct()

      concept_staged <-
        bind_rows(concept_staged_classes,
                  concept_staged_non_classes,
                  concept_staged_rxnorm)

      readr::write_csv(
        x = concept_staged,
        file = final_concept_csv
      )

      readr::write_csv(
        x = final_concept_relationship %>%
              dplyr::distinct(concept_code_1,
                              relationship_id,
                              concept_code_2,
                              relationship_source,
                              relationship_type),
        file = final_concept_relationship_csv
      )


      } else {

        final_concept_relationship <- concept_relationship_staged3
        final_concept_csv <-
          file.path(dir, "concept.csv")

        concept_staged1 <-
        final_concept_relationship %>%
          dplyr::filter(vocabulary_id_1 == class_type,
                        relationship_id == "Subsumes") %>%
          transmute(
            vocabulary_id =
              dplyr::case_when(
                vocabulary_id_1 %in% c("DISPOS", "STRUCT") ~ "SNOMEDCT_US",
                TRUE ~ vocabulary_id_1),
            concept_code = concept_code_1,
            concept_name = concept_name_1,
            concept_class = "Class") %>%
          distinct()


        concept_staged2 <-
        final_concept_relationship %>%
          dplyr::filter(vocabulary_id_2 == class_type,
                        relationship_id == "Subsumes") %>%
          transmute(
            vocabulary_id =
              dplyr::case_when(
                vocabulary_id_2 %in% c("DISPOS", "STRUCT") ~ "SNOMEDCT_US",
                TRUE ~ vocabulary_id_2),
            concept_code = concept_code_2,
            concept_name = concept_name_2,
            concept_class = "Concept") %>%
          distinct()

        concept_staged3 <-
          final_concept_relationship %>%
          dplyr::filter(vocabulary_id_2 == "RxNorm",
                        relationship_id == "Subsumes") %>%
          transmute(
            vocabulary_id = vocabulary_id_2,
            concept_code = concept_code_2,
            concept_name = concept_name_2,
            concept_class = "Concept") %>%
          distinct()

        concept_staged <-
          bind_rows(concept_staged1,
                    concept_staged2,
                    concept_staged3)

        readr::write_csv(
          x = concept_staged,
          file = final_concept_csv
        )

        readr::write_csv(
          x = final_concept_relationship %>%
            dplyr::distinct(concept_code_1,
                            relationship_id,
                            concept_code_2,
                            relationship_source,
                            relationship_type),
          file = final_concept_relationship_csv
        )

      }

      }


    }


    load_map <-
      file.path(getwd(),
                "inst",
                "RxClass API",
                version_key$version,
                "final",
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
        map(read_csv,
            col_types = readr::cols(.default = "c"),
            show_col_types = FALSE) %>%
        set_names(names(load_map[[i]]))


    }
    names(load_data) <-
      names(load_map)

    load_data <-
      load_data %>%
      map(bind_rows) %>%
      map(distinct)


    output_folder    <- "omop"
    output_subfolder <- as.character(Sys.time())


    path_vctr   <-
      c(getwd(),
        "inst",
        "RxClass API",
        version_key$version,
        output_folder,
        output_subfolder)

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
      mutate_at(vars(ends_with(".version")),
                ~ifelse(is.na(.), "", "X")) %>%
      rename(
        classType = classType.default,
        relaSources = relaSources.default,
        `version classType` = classType.version,
        `version relaSources` = relaSources.version
      )

    readme_df_as_lines <-
    capture.output(huxtable::print_screen(hux(readme_df), colnames = FALSE))

    # Huxtable centers table in output, and this replaces it with an indent
    readme_df_as_lines <-
      stringr::str_replace_all(
        string = readme_df_as_lines,
        pattern = "(^[ ]{1,})([A-Za-z]{1,}.*?)",
        replacement = "\t\\2"
      )

    readme_fn <- file.path(dir, "README")
    cat(
      "RxClass (setupRxNorm R package)",
      "Sourced from RxNav's RxClass API: https://lhncbc.nlm.nih.gov/RxNav/APIs/RxClassAPIs.html",
      "patelmeeray@gmail.com",
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
