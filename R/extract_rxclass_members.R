#' @title
#' Extract RxClass Members
#'
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
               "STRUCT")) {

    # Derived from https://lhncbc.nlm.nih.gov/RxNav/applications/RxClassIntro.html
    # to reduce the number of API calls needed per relaSource
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

      cli::cli_abort(
        "Relationships from {glue::glue_collapse(glue::single_quote(rela_sources), sep = ', ', last = ', and ')} to
        {glue::glue_collapse(glue::single_quote(class_types), sep = ', ', last = ', and ')} doesn't exist."
      )

    }

    class_types <- unique(lookup$classType)


    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {class_type}} {.file {members_csv}} ",
        "({cli::pb_current}/{cli::pb_total}) Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Wrote {cli::pb_total} csvs ",
        "in {cli::pb_elapsed}."
      ),
      total = nrow(lookup),
      clear = FALSE
    )

  version_key <- get_rxnav_api_version()
  for (ii in 1:nrow(lookup)) {

    class_type  <- lookup$classType[ii]
    rela_source <- lookup$relaSources[ii]
    path_vctr   <-
      c(getwd(),
        "inst",
        "RxClass API",
        version_key$version,
        "csv",
        class_type,
        "members")

    for (i in 1:length(path_vctr)) {

      dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

      if (!dir.exists(dir)) {

        dir.create(dir)
      }

    }


    members_csv <-
      file.path(dir, sprintf("%s.csv", rela_source))

    cli::cli_progress_update()
    Sys.sleep(0.01)

  if (!file.exists(members_csv)) {
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
    dir.create(tmp_dir)

    members_data <- load_rxclass_members(rela_sources = rela_source)
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
          map(tibble::as_tibble_row) %>%
          bind_rows() %>%
          pivot_wider(names_from = attrName,
                      values_from = attrValue,
                      values_fn = list)

        member_concept_df <-
        bind_cols(minConcept, nodeAttr) %>%
          unnest(everything())


        output[[bb]] <-
          member_concept_df


      }

      output <-
        output %>%
        bind_rows() %>%
        mutate(classId = class_id)


      readr::write_csv(
        x = output,
        file = file.path(tmp_dir, sprintf("%s.csv", class_id))
      )


    }

    final_output <-
    list.files(tmp_dir,
               full.names = TRUE) %>%
      map(read_csv,
          col_types = readr::cols(.default = "c")) %>%
      bind_rows()


    readr::write_csv(
      x = final_output,
      file = members_csv
    )

    unlink(tmp_dir,
           recursive = FALSE)

  }
  }

}
