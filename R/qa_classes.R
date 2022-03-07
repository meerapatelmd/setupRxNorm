member_concept_classes_csvs <-
  list.files("inst/RxClass API/07-Feb-2022/extracted/members/processed",
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
  map(read_csv, col_type = cols(.default = "c")) %>%
  set_names(rela_sources) %>%
  bind_rows(.id = "rela_source")

graph_concept_classes_csvs <-
list.files("inst/RxClass API/07-Feb-2022/extracted/graph/processed",
           full.names = TRUE,
           pattern = "concept.csv",
           recursive = TRUE)

graph_concept_classes_data <-
  graph_concept_classes_csvs %>%
  map(read_csv, col_type = cols(.default = "c")) %>%
  bind_rows


reconciled_data <-
  full_join(
    member_concept_classes_data,
    graph_concept_classes_data,
    by = c("concept_code", "standard_concept",  "class_type"),
    keep = TRUE,
    suffix = c(".members", ".graph")
  )


reconciled_data %>%
  dplyr::filter_at(vars(ends_with(".graph")),
                   all_vars(is.na(.))) %>%
  dplyr::transmute(
    concept_code = concept_code.members,
    concept_name = concept_name.members,
    class_type   = class_type.members,
    concept_class_id = 'Orphan',
    standard_concept = standard_concept.members) %>%
  dplyr::left_join(
    classtype_lookup %>%
      dplyr::transmute(
        class_type = classType,
        vocabulary_id = dplyr::coalesce(omop_vocabulary_id, custom_vocabulary_id)
      ),
    by = "class_type") %>%
  distinct()

RECONC
