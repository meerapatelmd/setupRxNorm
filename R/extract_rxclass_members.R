#' @title
#' Extract RxClass Members
#'
#' @details
#' RxClass Members represent the relationship between a Class belonging to
#' RxClass Graph and a Drug concept such as an RxNorm Ingredient.
#'
#' @rdname extract_rxclass_members
#' @export


extract_rxclass_members <-
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
               "STRUCT"),
           prior_version = NULL,
           prior_api_version = "3.1.174") {


    version_key <-
      list(version = prior_version,
           apiVersion = prior_api_version)


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    }


    cli::cli_h1(text = "RxClass Members -> Concept Relationship")

    # Derived from https://lhncbc.nlm.nih.gov/RxNav/applications/RxClassIntro.html
    # to reduce the number of API calls needed per relaSource
    full_lookup <-
      get_lookup()

    run_lookup <-
      full_lookup %>%
      dplyr::filter(relaSources %in% rela_sources) %>%
      dplyr::filter(classType %in% class_types)

    if (nrow(run_lookup)==0) {

      cli::cat_rule(cli::style_bold(cli::col_red(" * Error * ")),
                    line_col = "red")

      full_lookup %>%
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


      lookup <-
        dplyr::left_join(
          full_lookup,
          run_lookup,
          by = c("classType", "relaSources"),
          keep = TRUE,
          suffix = c("", ".run")) %>%
        dplyr::mutate_at(
          dplyr::vars(dplyr::ends_with(".run")),
          ~ifelse(is.na(.), "", "X")) %>%
        dplyr::left_join(
      get_rxnav_classes() %>%
        dplyr::count(classType),
      by = "classType") %>%
  dplyr::distinct() %>%
  dplyr::mutate(
    total_time_estimation =
      as.character(
        calculate_total_time(total_iterations = n))
  )


      print_lookup(lookup)



    }

    class_types <- unique(lookup$classType)

    cli::cli_h2("Saving Raw Members Data to CSV")

    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {class_type}} {.emph {rela_source}} ",
        "({cli::pb_current}/{cli::pb_total}) Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Extracted {nrow(lookup)} member{?s} to csvs.",
        "in {cli::pb_elapsed}"
      ),
      total = nrow(lookup),
      clear = TRUE
    )

  for (ii in 1:nrow(lookup)) {

    class_type_status  <- lookup$classType.run[ii]
    rela_source_status <- lookup$relaSources.run[ii]

    class_type  <- lookup$classType[ii]
    rela_source <- lookup$relaSources[ii]

    if (identical(class_type_status, "X") & identical(rela_source_status, "X")) {
    path_vctr   <-
      c(here::here(),
        "inst",
        "RxClass API",
        version_key$version,
        "extracted",
        "members",
        "raw",
        class_type)

    print_df(df = lookup,
             highlight_row = ii)

    cat(
    glue::glue(
      '\textracted /',
      '\t  members /',
      '\t    raw /',
      '\t      {class_type} /',
      '\t        {rela_source}.csv',
      '\t    processed /',
      '\t      {class_type} /',
      '\t        {rela_source} /',
      '\t          CONCEPT_RELATIONSHIP.csv',
      '\t          CONCEPT_CONCEPTS.csv',
      '\t          CONCEPT_SYNONYM_CONCEPTS.csv',
      '\t          CONCEPT_CLASSES.csv',
      .sep = "\n"),
    sep = "\n")


    for (i in 1:length(path_vctr)) {

      dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

      if (!dir.exists(dir)) {

        dir.create(dir)
      }

    }


    raw_members_csv <-
      file.path(dir, sprintf("%s.csv", rela_source))

    cli::cli_progress_update()

  if (!file.exists(raw_members_csv)) {
    cli_file_missing(file_path = raw_members_csv)
    #cli::cli_text("[{as.character(Sys.time())}] {.file {raw_members_csv}} ")
    tmp_path_vctr <-
      c(path_vctr,
        "tmp",
        rela_source)

    for (i in 1:length(tmp_path_vctr)) {

      tmp_dir <- paste(tmp_path_vctr[1:i], collapse = .Platform$file.sep)

      if (!dir.exists(tmp_dir)) {

        dir.create(tmp_dir)

      }

    }

    unlink(tmp_dir, recursive = TRUE)
    unlink(tmp_dir)
    dir.create(tmp_dir)
    on.exit(drop_dir(tmp_dir),
            add = TRUE,
            after = TRUE)


    members_data <- load_rxclass_members(rela_sources = rela_source,
                                         class_types = class_type,
                                         prior_version = version_key$version,
                                         prior_api_version = version_key$apiVersion)

    cli_message(glue::glue("Processing {length(members_data)} {class_type} members in {rela_source}..."))
    for (aa in seq_along(members_data)) {
      output <- list()
      class_id <- names(members_data)[aa]
      member_concepts_data <-
        members_data[[aa]]$drugMemberGroup$drugMember

      for (bb in seq_along(member_concepts_data)) {

        minConcept <-
        member_concepts_data[[bb]]$minConcept %>%
          tibble::as_tibble_row()

        nodeAttr <-
        member_concepts_data[[bb]]$nodeAttr %>%
          purrr::map(tibble::as_tibble_row) %>%
          dplyr::bind_rows() %>%
          tidyr::pivot_wider(names_from = attrName,
                      values_from = attrValue,
                      values_fn = list)

        member_concept_df <-
        dplyr::bind_cols(minConcept, nodeAttr) %>%
          tidyr::unnest(dplyr::everything())


        output[[bb]] <-
          member_concept_df


      }

      output <-
        output %>%
        dplyr::bind_rows() %>%
        dplyr::mutate(classId = class_id)


      readr::write_csv(
        x = output,
        file = file.path(tmp_dir, sprintf("%s.csv", class_id))
      )


    }

    final_output <-
    list.files(tmp_dir,
               full.names = TRUE) %>%
      purrr::map(read_csv,
          col_types = readr::cols(.default = "c")) %>%
      dplyr::bind_rows()


    readr::write_csv(
      x = final_output,
      file = raw_members_csv
    )

    unlink(tmp_dir,
           recursive = TRUE)

    unlink(tmp_dir)

    cli_missing_file_written(raw_members_csv)

  } else {
  cli_file_exists(file_path = raw_members_csv)

  }


    raw_source_path <-
      file.path(
        here::here(),
        "inst",
        "RxClass API",
        version_key$version,
        "extracted",
        "members",
        "raw",
        class_type)

    path_vctr   <-
      c(here::here(),
        "inst",
        "RxClass API",
        version_key$version,
        "extracted",
        "members",
        "processed",
        class_type,
        rela_source)

    for (i in 1:length(path_vctr)) {

      dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

      if (!dir.exists(dir)) {

        dir.create(dir)
      }

    }

    print_df(lookup,
             highlight_row = ii)


    raw_members_csv <-
      file.path(raw_source_path, sprintf("%s.csv", rela_source))

    concept_csv <-
      file.path(dir, "CONCEPT_CONCEPTS.csv")
    concept_synonym_concepts_csv <-
      file.path(dir, "CONCEPT_SYNONYM_CONCEPTS.csv")
    concept_classes_csv <-
      file.path(dir, "CONCEPT_CLASSES.csv")
    cr_csv <-
      file.path(dir, "CONCEPT_RELATIONSHIP.csv")

    if (!file.exists(concept_csv)|
        !file.exists(concept_synonym_concepts_csv)|
        !file.exists(concept_classes_csv)|
        !file.exists(cr_csv)) {

      cli_file_missing(concept_csv)

      raw_members_data <-
        readr::read_csv(
          file = raw_members_csv,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )

      members_data <-
        raw_members_data %>%
        dplyr::transmute(
          rxnorm_concept_code = rxcui,
          rxnorm_concept_name = name,
          rxnorm_concept_class_id = tty,
          rxnorm_standard_concept = NA_character_,
          source_concept_code = SourceId,
          source_concept_name = SourceName,
          source_standard_concept = NA_character_,
          # source_vocabulary_id = rela_source,
          source_standard_concept = NA_character_,
          relationship_source = rela_source,
          relationship_type   = Relation,
          class_concept_code  = classId,
          class_standard_concept = "C",
          class_class_type    = class_type) %>%
        dplyr::left_join(
          relasource_vocabulary_lookup %>%
            dplyr::transmute(
              relationship_source = relaSources,
              source_vocabulary_id = dplyr::coalesce(omop_vocabulary_id, custom_vocabulary_id)),
          by = "relationship_source") %>%
        dplyr::distinct() %>%
        dplyr::select(
          rxnorm_concept_code,
          rxnorm_concept_name,
          rxnorm_concept_class_id,
          rxnorm_standard_concept,
          source_concept_code,
          source_concept_name,
          source_standard_concept,
          source_vocabulary_id,
          relationship_source,
          relationship_type,
          class_concept_code,
          class_standard_concept,
          class_class_type,
          relationship_source,
          relationship_type
        )

      concept_concepts0 <-
        dplyr::bind_rows(
          members_data %>%
            dplyr::transmute(
              concept_code  = rxnorm_concept_code,
              concept_name  = rxnorm_concept_name,
              class_type    = class_type,
              concept_class_id = rxnorm_concept_class_id,
              standard_concept = rxnorm_standard_concept,
              vocabulary_id = 'RxNorm') %>%
            dplyr::distinct(),
          members_data %>%
            dplyr::transmute(
              concept_code  = source_concept_code,
              concept_name  = source_concept_name,
              class_type    = class_type,
              concept_class_id = "Concept",
              standard_concept = source_standard_concept,
              vocabulary_id =  source_vocabulary_id) %>%
            dplyr::distinct()
        )

      concept_concepts0 <-
        concept_concepts0 %>%
        dplyr::group_by(concept_code, vocabulary_id) %>%
        dplyr::arrange(concept_name, .by_group = TRUE) %>%
        dplyr::mutate(concept_name_rank = 1:dplyr::n()) %>%
        dplyr::ungroup()


      concept_concepts <-
        concept_concepts0 %>%
        dplyr::filter(concept_name_rank == 1) %>%
        dplyr::select(-concept_name_rank) %>%
        dplyr::distinct()


      readr::write_csv(
        file = concept_csv,
        x = concept_concepts
      )

      cli_missing_file_written(concept_csv)

      concept_synonym_concepts <-
        concept_concepts0 %>%
        dplyr::filter(concept_name_rank != 1) %>%
        dplyr::transmute(
          concept_code,
          concept_synonym_name = concept_name,
          class_type) %>%
        distinct()

      readr::write_csv(
        x = concept_synonym_concepts,
        file = concept_synonym_concepts_csv
      )

      cli_missing_file_written(concept_synonym_concepts_csv)

      concept_classes <-
        members_data %>%
        dplyr::transmute(
          concept_code  = class_concept_code,
          standard_concept = class_standard_concept,
          class_type = class_class_type) %>%
        dplyr::distinct()

      readr::write_csv(
        x = concept_classes,
        file = concept_classes_csv
      )

      cli_missing_file_written(concept_classes_csv)

      cr <-
        dplyr::bind_rows(
          members_data %>%
            dplyr::transmute(
              concept_code_1  = rxnorm_concept_code,
              class_type_1    = class_type,
              relationship_id = 'Mapped from',
              relationship_source,
              relationship_type = NA_character_,
              concept_code_2  = source_concept_code,
              class_type_2    = class_type) %>%
            distinct(),
          members_data %>%
            dplyr::transmute(
              concept_code_1  = source_concept_code,
              class_type_1    = class_type,
              relationship_id = 'Maps to',
              relationship_source,
              relationship_type = NA_character_,
              concept_code_2  = rxnorm_concept_code,
              class_type_2    = class_type) %>%
            distinct(),
          members_data %>%
            dplyr::transmute(
              concept_code_1  = class_concept_code,
              class_type_1    = class_type,
              relationship_id = 'Subsumes',
              relationship_source,
              relationship_type,
              concept_code_2  = rxnorm_concept_code,
              class_type_2    = class_type) %>%
            distinct()) %>%
        dplyr::filter(!is.na(concept_code_1),
                      !is.na(concept_code_2))


      readr::write_csv(
        x = cr,
        file = cr_csv
      )

      cli_missing_file_written(cr_csv)


    } else {

      cli_file_exists(concept_csv)

      cli_file_exists(concept_synonym_concepts_csv)

      cli_file_exists(concept_classes_csv)

      cli_file_exists(cr_csv)


    }


    }




  }

}
