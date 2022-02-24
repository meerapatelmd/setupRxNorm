#' @title
#' Extract RxClass Graph
#'
#' @details
#' Some class types such as DISEASE follow a cyclical hierarchical pattern that
#' can recurse into infinity if a limit is imposed when writing the Concept Ancestor
#' csv file. Concept Ancestor processing is completed when a subsequent level of
#' parent and children is a dataframe that is a duplicate of a previous level's.
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
    "DISPOS")) {


    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {classType}} {.file {fileType}} ",
        "({cli::pb_current}/{cli::pb_total}) Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Wrote {cli::pb_total} csvs ",
        "in {cli::pb_elapsed}."
      ),
      total = length(class_types)*3, # 3 for nodes, edges, and concept_ancestor
      clear = FALSE
    )

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

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "node.csv"
  cli::cli_progress_update()
  Sys.sleep(0.01)

  if (!file.exists(class_type_node_csv)) {

    class_type_data <- load_rxclass_graph(class_types = class_type)

    readr::write_csv(
      x = class_type_data$NODE,
      file = class_type_node_csv
    )

  }

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "edge.csv"
  cli::cli_progress_update()
  Sys.sleep(0.01)

  class_type_edge_csv <-
    file.path(dir, "edge.csv")

  if (!file.exists(class_type_edge_csv)) {

    class_type_data <- load_rxclass_graph(class_types = class_type)

    readr::write_csv(
      x = class_type_data$EDGE,
      file = class_type_edge_csv
    )

  }

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "concept_ancestor.csv"
  cli::cli_progress_update()
  Sys.sleep(0.01)

  class_type_concept_ancestor_csv <-
    file.path(dir, "concept_ancestor.csv")

  if (!file.exists(class_type_concept_ancestor_csv)) {


    #class_type_data <- load_rxclass_graph(class_types = class_type)
    node <- readr::read_csv(class_type_node_csv, show_col_types = FALSE)
    edge <-
      readr::read_csv(class_type_edge_csv, show_col_types = FALSE) %>%
      # filter for hierarchical relationships
      dplyr::filter(rela == 'isa')


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
      dplyr::filter(!(parent %in% child)) %>%
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


    output2 <-
      output %>%
      map(select, -rela) %>%
      reduce(left_join) %>%
      select(starts_with("parent_")) %>%
      rename_all(str_remove_all, "parent_") %>%
      dplyr::distinct()

    # Recurse for each column level
    # Since number of columns is driven by loop above, it
    # needs to extracted for the recursion
    level_cols <- colnames(output2)

    output2 <-
      output2 %>%
      # Adding an identifier per path to recurse later
      rowid_to_column("rowid")


    output3 <-
      split(output2,
            output2$rowid) %>%
      map(select, -rowid)

    # For each path, need to recurse through the
    # different levels
    output4 <- list()
    for (a in seq_along(output3)) {
      output4[[a]] <- list()
      path_row <- output3[[a]]
      path_row <-
      path_row %>%
        pivot_longer(cols = everything(),
                     names_to = "level_of_separation",
                     values_to = "descendant_concept_code",
                     values_drop_na = TRUE,
                     names_transform = as.integer) %>%
        select(descendant_concept_code,
               level_of_separation) %>%
        # Offset levels by 1
        mutate(level_of_separation =
                 level_of_separation-1)

      for (b in 1:nrow(path_row)) {

        if (b == 1) {

          ancestor_concept_code <-
            path_row$descendant_concept_code[b]

          output4[[a]][[1]] <-
            path_row %>%
            dplyr::transmute(
              ancestor_concept_code = ancestor_concept_code,
              descendant_concept_code,
              level_of_separation
            )

        } else {

          ancestor_concept_code_b <-
            output4[[a]][[b-1]]$descendant_concept_code[2]

          output4[[a]][[b]] <-
            output4[[a]][[b-1]] %>%
            slice(-1) %>%
            dplyr::transmute(
              ancestor_concept_code = ancestor_concept_code_b,
              descendant_concept_code,
              level_of_separation = level_of_separation-1
            )


        }
      }
    }

    output5 <-
      output4 %>%
      map(bind_rows)


    output6 <-
      bind_rows(output5) %>%
      dplyr::distinct()


    readr::write_csv(
      x = output6,
      file = class_type_concept_ancestor_csv
    )


      }


    }


  }

