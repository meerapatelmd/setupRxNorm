extract_rxclass_graph <-
  function(
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
    "CHEM",
    "SCHEDULE",
    "STRUCT",
    "DISPOS")) {

for (class_type in class_types) {

  version_key <- get_rxnav_api_version()
  path_vctr   <- c(getwd(), "inst", "RxClass API", version_key$version, "csv", class_type)

  for (i in 1:length(path_vctr)) {

    dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

    if (!dir.exists(dir)) {

      dir.create(dir)
    }

  }

  class_type_node_csv <-
    file.path(dir, "node.csv")

  if (!file.exists(class_type_node_csv)) {

    class_type_data <- load_rxclass_graph(class_types = class_type)

    readr::write_csv(
      x = class_type_data$NODE,
      file = class_type_node_csv
    )

  }

  class_type_edge_csv <-
    file.path(dir, "edge.csv")

  if (!file.exists(class_type_edge_csv)) {

    class_type_data <- load_rxclass_graph(class_types = class_type)

    readr::write_csv(
      x = class_type_data$EDGE,
      file = class_type_edge_csv
    )

  }

}

  }
