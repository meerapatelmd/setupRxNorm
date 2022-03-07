read_members_concept_classes_csvs <-
  function() {

    version_key <-
      get_rxnav_api_version()

    extracted_members_processed_path <-
      file.path(getwd(),
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

    member_concept_classes_data

  }
