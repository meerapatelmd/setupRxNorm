read_graph_concept_csvs <-
function(prior_version = NULL,
         prior_api_version = "3.1.174") {

  version_key <-
    list(version = prior_version,
         apiVersion = prior_api_version)


  if (is.null(prior_version)) {

    version_key <- get_rxnav_api_version()

  }

  extracted_graph_processed_path <-
    file.path(getwd(),
              "inst",
              "RxClass API",
              version_key$version,
              "extracted",
              "graph",
              "processed")

  graph_concept_classes_csvs <-
    list.files(extracted_graph_processed_path,
               full.names = TRUE,
               pattern = "CONCEPT.csv",
               recursive = TRUE)

  graph_concept_classes_data <-
    graph_concept_classes_csvs %>%
    purrr::map(read_csv, col_type = cols(.default = "c")) %>%
    dplyr::bind_rows()

  graph_concept_classes_data

}
