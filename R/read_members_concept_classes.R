read_members_concept_classes_csvs <-
  function(prior_version = NULL,
           prior_api_version = "3.1.174") {

    # prior_version <- "07-Feb-2022"
    # prior_api_version <- "3.1.174"

    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    } else {

      version_key <-
        list(version = prior_version,
             apiVersion = prior_api_version)

    }

    extracted_members_processed_path <-
      file.path(here::here(),
                "inst",
                "RxClass API",
                version_key$version,
                "extracted",
                "members",
                "processed")

    member_concept_classes_csvs <-
      list.files(extracted_members_processed_path,
                 full.names = TRUE,
                 pattern = "CONCEPT_CLASSES.csv",
                 recursive = TRUE)

    rela_sources <-
      stringr::str_replace_all(
        member_concept_classes_csvs,
        pattern = "(^.*)/(.*?)/CONCEPT_CLASSES.csv",
        replacement = "\\2"
      )

    member_concept_classes_data <-
      member_concept_classes_csvs %>%
      purrr::map(readr::read_csv, col_type = readr::cols(.default = "c")) %>%
      purrr::set_names(rela_sources) %>%
      dplyr::bind_rows(.id = "rela_source")

    member_concept_classes_data %>%
      dplyr::filter(rela_source %in% unique(get_lookup()$relaSources))

  }
