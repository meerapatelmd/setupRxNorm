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
               # "MOA",
               # "PE",
               "PK",
               "TC",
               # "VA",
               # "DISEASE",
               # "DISPOS",
               # "CHEM",
               "SCHEDULE" # ,
               # "STRUCT"
               )) {


    version_key <- get_rxnav_api_version()

    for (class_type in class_types) {

      extract_rxclass_graph(class_types = class_type)
      extract_rxclass_members(class_types = class_type)

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
          show_col_types = FALSE) %>%
        transmute(concept_code_1 = classId,
                  concept_name_1 = className)


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

      readr::write_csv(
        x = concept_relationship_staged3,
        file = final_concept_relationship_csv
      )


      }


      final_concept_ancestor <-
      readr::read_csv(
      final_concept_ancestor_csv,
      col_types = readr::cols(.default = "c"),
      show_col_types = FALSE)

      final_concept_relationship <-
        readr::read_csv(
          final_concept_relationship_csv,
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE)

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
          standard_concept = "C") %>%
        distinct()


      final_concept_relationship %>%
        dplyr::filter(vocabulary_id_1 == class_type,
                      relationship_id != "Subsumes",
                      !(concept_code_1 %in% concept_staged_classes$concept_code)) %>%
        count(vocabulary_id_1,
              relationship_id,
              vocabulary_id_2)

      # QA
      # Are all the terminal concept codes from the source found as descendant concept codes?

      final_concept_ancestor %>%
        distinct(descendant_concept_code) %>%
        mutate(concept_code = descendant_concept_code) %>%
        full_join(final_concept_relationship %>%
                    dplyr::filter(vocabulary_id_1 == class_type,
                                  relationship_id == "Subsumes") %>%
                    distinct(concept_code_1) %>%
                    mutate(concept_code = concept_code_1),
                  by = "concept_code") %>%
        mutate(category =
                 case_when(is.na(concept_code_1)  & !is.na(descendant_concept_code) ~ "no_members",
                          !is.na(concept_code_1)  &  is.na(descendant_concept_code) ~ "no_classes",
                           TRUE ~ "")) %>%
        count(category)

      final_concept_ancestor %>%
        distinct(ancestor_concept_code) %>%
        mutate(concept_code = ancestor_concept_code) %>%
        full_join(final_concept_relationship %>%
                    dplyr::filter(vocabulary_id_1 == class_type,
                                  relationship_id == "Subsumes") %>%
                    distinct(concept_code_1) %>%
                    mutate(concept_code = concept_code_1),
                  by = "concept_code") %>%
        mutate(category =
                 case_when(is.na(concept_code_1)  & !is.na(ancestor_concept_code) ~ "no_members",
                           !is.na(concept_code_1)  &  is.na(ancestor_concept_code) ~ "no_classes",
                           TRUE ~ "")) %>%
        count(category)

}



  }
