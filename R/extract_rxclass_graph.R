#' @title
#' Extract RxClass Graph
#'
#' @details
#' Some class types such as DISEASE follow a cyclical hierarchical pattern that
#' can recurse into infinity if a limit is imposed when writing the Concept Ancestor
#' csv file. Concept Ancestor processing is completed when a subsequent level of
#' parent and children is a dataframe that is a duplicate of a previous level's.
#'
#' Other class types such as SCHEDULE do not have multiple levels.
#'
#' The response from the RxNav RxClass API is a set of nodes and edges for a given
#' `class_type`. Both are saved as csvs to the 'raw' folder.
#'
#' The edge.csv is then processed into a Concept Ancestor format as 'CONCEPT_ANCESTOR.csv'
#' under the 'processed' folder.
#'
#' @return
#' For each `class_type`, the following csvs are returned to the installation dir:
#' RxClass API /
#'   \emph{RxClass Version} /
#'     extracted /
#'       graph /
#'         raw /
#'           node.csv
#'           edge.csv
#'         processed /
#'           CONCEPT_ANCESTOR.csv
#'
#' The CONCEPT_ANCESTOR.csv is processed to include levels of separation with the
#' source edge.csv fields classId1 and classId2 translated to child and parent,
#' respectively.
#'
#' @rdname extract_rxclass_graph
#' @export


extract_rxclass_graph <-
  function(class_types =
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
    "DISPOS"),
  prior_version = NULL,
  prior_api_version = "3.1.174") {

    version_key <-
      list(version = prior_version,
           apiVersion = prior_api_version)


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    }

    cli::cli_h1(text = "RxClass Graph")


    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {classType}} {.file {fileType}} ",
        "({cli::pb_current}/{cli::pb_total}) Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Wrote {cli::pb_total} csvs ",
        "in {cli::pb_elapsed}."
      ),
      total = length(class_types)*4, # 4 for nodes, edges, concept_ancestor, and concept (tandem concept_synonym)
      clear = FALSE
    )

for (class_type in class_types) {

  path_vctr   <- c(here::here(), "inst", "RxClass API", version_key$version, "extracted", "graph", "raw", class_type)

  for (i in 1:length(path_vctr)) {

    dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

    if (!dir.exists(dir)) {

      dir.create(dir)
    }

  }

  class_type_node_csv <-
    file.path(dir, "node.csv")
  cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_node_csv}} ")

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "node.csv"
  cli::cli_progress_update()

  if (!file.exists(class_type_node_csv)) {

    class_type_data <- load_rxclass_graph(class_types = class_type,
                                          prior_version = version_key$version,
                                          prior_api_version = version_key$apiVersion)

    readr::write_csv(
      x = dplyr::distinct(class_type_data$NODE),
      file = class_type_node_csv
    )

  }

  class_type_edge_csv <-
    file.path(dir, "edge.csv")
  cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_edge_csv}} ")

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "edge.csv"
  cli::cli_progress_update()

  if (!file.exists(class_type_edge_csv)) {

    class_type_data <- load_rxclass_graph(class_types = class_type,
                                          prior_version = version_key$version,
                                          prior_api_version = version_key$apiVersion)

    readr::write_csv(
      x = class_type_data$EDGE,
      file = class_type_edge_csv
    )

  }

  path_vctr   <- c(here::here(), "inst", "RxClass API", version_key$version, "extracted", "graph", "processed", class_type)

  for (i in 1:length(path_vctr)) {

    dir <- paste(path_vctr[1:i], collapse = .Platform$file.sep)

    if (!dir.exists(dir)) {

      dir.create(dir)
    }

  }

  class_type_concept_ancestor_csv <-
    file.path(dir, "CONCEPT_ANCESTOR.csv")
  cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_concept_ancestor_csv}} ")

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "CONCEPT_ANCESTOR.csv"
  cli::cli_progress_update()

  if (!file.exists(class_type_concept_ancestor_csv)) {


    edge <-
      readr::read_csv(class_type_edge_csv, col_types = readr::cols(.default = "c"), show_col_types = FALSE)

    if (nrow(edge) == 0) {

      readr::write_csv(
        x = edge,
        file = class_type_concept_ancestor_csv
      )


    } else {



    # Rename edge fields to indicate directionality
    # ClassId1 --> child
    # ClassId2 --> parent

    edge2 <-
      edge %>%
      dplyr::transmute(
        parent = classId2,
        rela,
        child = classId1
      )
    # get the roots
    root <-
      edge2 %>%
      dplyr::filter(!(parent %in% edge2$child)) %>%
      dplyr::distinct(parent) %>%
      unlist() %>%
      unname()

    continue <- TRUE
    j <- 0
    output <- list()
    while (continue == TRUE) {

      j <- j + 1

      if (j == 1) {

        output[[1]] <-
          edge2 %>%
          dplyr::filter(parent %in% root) %>%
          dplyr::transmute(
            parent_1 = parent,
            child_1 = child,
            rela,
            parent_2 = child
          )

      } else {

        next_parents <-
          unique(unlist(output[[j-1]][,sprintf("parent_%s", j)]))

        next_children <-
          edge2 %>%
          dplyr::filter(parent %in% next_parents) %>%
          dplyr::distinct() %>%
          dplyr::transmute(
            parent,
            rela,
            child,
            next_parent = child)

        colnames(next_children) <-
          c(
            sprintf("parent_%s", j),
            "rela",
            sprintf("child_%s", j),
            sprintf("parent_%s", j+1)
          )

        # Has the dataframe already been retrieved?
        # If this is a duplicate dataframe, this means that there might be a
        # possible cyclical relationship in the hierarchy
        qa_output <-
        output %>%
          purrr::map(unname) %>%
          purrr::map(function(x) identical(x, unname(next_children))) %>%
          purrr::keep(~.==TRUE)

        if (nrow(next_children) == 0|length(qa_output)>0) {

          continue <- FALSE

        } else {

          output[[j]] <- next_children

        }


      }



    }

    tmp_ca_dir <-
      file.path(dir, "tmp")

    if (dir.exists(tmp_ca_dir)) {

      unlink(tmp_ca_dir, recursive = TRUE)
      unlink(tmp_ca_dir)

    }
    dir.create(tmp_ca_dir)


    tmp_ca_files <- vector()
    for (i in seq_along(output)) {

      tmp_ca_file <-
        file.path(tmp_ca_dir, sprintf("%s.csv", i))

      readr::write_csv(
        x = output[[i]] %>% select(-rela),
        file = tmp_ca_file
      )

      tmp_ca_files <-
      c(tmp_ca_files,
        tmp_ca_file)

    }

    tmp_concept_ancestor_csv <-
      file.path(dir, "tmp_concept_ancestor.csv")

    # SCHEDULE class type does not have multiple levels
    if (length(tmp_ca_files) == 1) {
      tmp_concept_ancestor0 <-
      readr::read_csv(tmp_ca_files[1],
                      col_types = readr::cols(.default = "c"),
                      show_col_types = FALSE)

      readr::write_csv(
        x = tmp_concept_ancestor0,
        file = tmp_concept_ancestor_csv
      )

    } else if (length(tmp_ca_files)==2) {

    tmp_concept_ancestor0 <-
      dplyr::left_join(
        readr::read_csv(tmp_ca_files[1],
                        col_types = readr::cols(.default = "c"),
                        show_col_types = FALSE),
        readr::read_csv(tmp_ca_files[2],
                        col_types = readr::cols(.default = "c"),
                        show_col_types = FALSE)) %>%
      dplyr::distinct()

    readr::write_csv(
      x = tmp_concept_ancestor0,
      file = tmp_concept_ancestor_csv
    )


    } else {

      tmp_concept_ancestor0 <-
        dplyr::left_join(
          readr::read_csv(tmp_ca_files[1],
                          col_types = readr::cols(.default = "c"),
                          show_col_types = FALSE),
          readr::read_csv(tmp_ca_files[2],
                          col_types = readr::cols(.default = "c"),
                          show_col_types = FALSE)) %>%
        dplyr::distinct()

      tmp_concept_ancestor <- list()
      tmp_concept_ancestor[[1]] <-
        tmp_concept_ancestor0

      for (xx in 3:length(tmp_ca_files)) {



          tmp_concept_ancestor[[length(tmp_concept_ancestor)+1]] <-
          dplyr::left_join(
          tmp_concept_ancestor[[length(tmp_concept_ancestor)]],
          readr::read_csv(tmp_ca_files[xx],
                          col_types = readr::cols(.default = "c"),
                          show_col_types = FALSE)) %>%
            dplyr::distinct()


      }

    readr::write_csv(
      x = tmp_concept_ancestor[[length(tmp_concept_ancestor)]],
      file = tmp_concept_ancestor_csv
    )

    }

    # Max Level is needed to know the
    # endpoint of the loop later on
    max_level <- length(tmp_ca_files)

    tmp_concept_ancestor <-
      readr::read_csv(tmp_concept_ancestor_csv,
                      col_types = readr::cols(.default = "c"))

    unlink(tmp_ca_dir, recursive = TRUE)
    dir.create(tmp_ca_dir)

    tmp_concept_ancestor2 <-
      tmp_concept_ancestor %>%
      dplyr::select(dplyr::starts_with("parent_")) %>%
      dplyr::rename_all(stringr::str_remove_all, "parent_") %>%
      dplyr::distinct()

    tmp_concept_ancestor3 <-
      tmp_concept_ancestor2 %>%
      # Adding an identifier per path to recurse later
      tibble::rowid_to_column("rowid")

    readr::write_csv(
      x = tmp_concept_ancestor3,
      file = tmp_concept_ancestor_csv
    )

    tmp_concept_ancestor4 <-
      tidyr::pivot_longer(tmp_concept_ancestor3,
                   cols = !rowid,
                   names_to = "levels_of_separation",
                   values_to = "descendant_concept_code",
                   values_drop_na = FALSE,
                   names_transform = as.integer)

    tmp_concept_ancestor5 <- list()
    for (i in 1:max_level) {

      # Ancestor of a given rowid at the level i
      tmp_concept_ancestor5[[i]] <-
      tmp_concept_ancestor4 %>%
        dplyr::group_by(rowid) %>%
        dplyr::filter(levels_of_separation == i) %>%
        dplyr::ungroup() %>%
        dplyr::filter(!is.na(descendant_concept_code)) %>%
        dplyr::transmute(rowid,
                  ancestor_concept_code = descendant_concept_code) %>%
        dplyr::distinct() %>%
        dplyr::left_join(tmp_concept_ancestor4,
                  by = "rowid") %>%
        dplyr::filter(!is.na(descendant_concept_code)) %>%
        distinct() %>%
        dplyr::mutate(levels_of_separation =
                        levels_of_separation-i) %>%
        dplyr::filter(levels_of_separation >= 0)

    }

    tmp_concept_ancestor6 <-
      dplyr::bind_rows(tmp_concept_ancestor5) %>%
      dplyr::select(-rowid) %>%
      dplyr::distinct() %>%
      dplyr::group_by(ancestor_concept_code,
               descendant_concept_code) %>%
      dplyr::summarize(min_levels_of_separation = min(levels_of_separation),
                max_levels_of_separation = max(levels_of_separation),
                .groups = "drop") %>%
      dplyr::ungroup()

    readr::write_csv(
      x =  tmp_concept_ancestor6,
      file = class_type_concept_ancestor_csv
    )

    unlink(tmp_ca_dir, recursive = TRUE)
    file.remove(tmp_concept_ancestor_csv)

    }


      }



  class_type_concept_csv <-
    file.path(dir, "CONCEPT.csv")
  cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_concept_csv}} ")

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "CONCEPT.csv"
  cli::cli_progress_update()


  if (!file.exists(class_type_concept_csv)) {
  graph_data <-
    list(
      node = class_type_node_csv,
      edge = class_type_edge_csv,
      concept_ancestor = class_type_concept_ancestor_csv) %>%
    map(readr::read_csv,
        col_types = readr::cols(.default = "c"),
        show_col_types = FALSE)


  # STRUCT has 0 nodes and 0 edges so blank concept synonym and concept csvs
  # will be written later

  if (nrow(graph_data$node)==0 & nrow(graph_data$edge)==0) {

    concept <-
      tibble::tribble(
        ~concept_code,
        ~concept_name,
        ~class_type,
        ~concept_class_id,
        ~standard_concept
      )
    readr::write_csv(
      x =  concept,
      file = class_type_concept_csv
    )

    concept_synonym <-
      tibble::tribble(
        ~concept_code,
        ~concept_synonym_name,
        ~class_type
      )

    class_type_concept_synonym_csv <-
      file.path(dir, "CONCEPT_SYNONYM.csv")
    cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_concept_synonym_csv}} ")

    readr::write_csv(
      x =  concept_synonym,
      file = class_type_concept_synonym_csv
    )


  } else {



  node_check <-
    list(
      graph_data$node %>%
        dplyr::distinct(classId) %>%
        dplyr::transmute(node_in_node_csv = classId,
                  classId),
      graph_data$edge %>%
        dplyr::distinct(classId1) %>%
        dplyr::transmute(node1_in_edge_csv = classId1,
                  classId = classId1),
      graph_data$edge %>%
        dplyr::distinct(classId2) %>%
        dplyr::transmute(node2_in_edge_csv = classId2,
                  classId = classId2),
      graph_data$concept_ancestor %>%
        dplyr::distinct(ancestor_concept_code) %>%
        dplyr::transmute(ancestor_in_ca_csv = ancestor_concept_code,
                  classId = ancestor_concept_code),
      graph_data$concept_ancestor %>%
        dplyr::distinct(descendant_concept_code) %>%
        dplyr::transmute(descendant_in_ca_csv = descendant_concept_code,
                  classId = descendant_concept_code)) %>%
    purrr::reduce(dplyr::full_join, by = "classId") %>%
    dplyr::transmute(
      all_classId = classId,
      node_in_node_csv,
      node1_in_edge_csv,
      node2_in_edge_csv,
      ancestor_in_ca_csv,
      descendant_in_ca_csv)

  concept_map <-
  node_check %>%
    dplyr::mutate(
      node_class_type =
        dplyr::case_when(
          is.na(node1_in_edge_csv) & !is.na(node2_in_edge_csv) ~ "Root",
          !is.na(node1_in_edge_csv) & is.na(node2_in_edge_csv) ~ "Leaf",
          !is.na(node1_in_edge_csv) & !is.na(node2_in_edge_csv) ~ "SubClass",
          TRUE ~ "(Node Not Found in Edge)")) %>%
    dplyr::select(concept_code =
             all_classId,
           concept_class_id = node_class_type) %>%
    dplyr::distinct() %>%
    dplyr::mutate(standard_concept = 'C')


  concept0 <-
  graph_data$node %>%
    dplyr::select(concept_code = classId,
           concept_name = className,
           class_type   = classType) %>%
    dplyr::left_join(concept_map,
              by = "concept_code") %>%
    dplyr::distinct()

  # Dedupe codes
  concept0 <-
  concept0 %>%
    dplyr::group_by(concept_code,
             class_type) %>%
    dplyr::arrange(concept_name,
                   .by_group = TRUE) %>%
    dplyr::mutate(concept_name_rank = 1:dplyr::n()) %>%
    dplyr::ungroup()

  concept <-
    concept0 %>%
    dplyr::filter(concept_name_rank == 1) %>%
    dplyr::select(-concept_name_rank) %>%
    dplyr::left_join(
      classtype_lookup %>%
        dplyr::transmute(
          class_type = classType,
          vocabulary_id =
            dplyr::coalesce(omop_vocabulary_id, custom_vocabulary_id)),
      by = "class_type") %>%
    dplyr::distinct()

  readr::write_csv(
    x =  concept,
    file = class_type_concept_csv
  )

  concept_synonym <-
    concept0 %>%
    dplyr::filter(concept_name_rank != 1) %>%
    dplyr::select(concept_code,
                  concept_synonym_name = concept_name,
                  class_type) %>%
    dplyr::distinct()

  class_type_concept_synonym_csv <-
    file.path(dir, "CONCEPT_SYNONYM.csv")
  cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_concept_synonym_csv}} ")

  readr::write_csv(
    x =  concept_synonym,
    file = class_type_concept_synonym_csv
  )








  }

  }

}


  }

