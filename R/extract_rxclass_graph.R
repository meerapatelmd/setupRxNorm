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
      total = length(class_types)*3, # 3 for nodes, edges, and concept_ancestor
      clear = FALSE
    )

for (class_type in class_types) {

  version_key <- get_rxnav_api_version()
  path_vctr   <- c(getwd(), "inst", "RxClass API", version_key$version, "extracted", "graph", class_type)

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

    class_type_data <- load_rxclass_graph(class_types = class_type)

    readr::write_csv(
      x = class_type_data$NODE,
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

    class_type_data <- load_rxclass_graph(class_types = class_type)

    readr::write_csv(
      x = class_type_data$EDGE,
      file = class_type_edge_csv
    )

  }

  class_type_concept_ancestor_csv <-
    file.path(dir, "concept_ancestor.csv")
  cli::cli_text("[{as.character(Sys.time())}] {.file {class_type_concept_ancestor_csv}} ")

  # objects for the progress bar
  classType <- class_type
  fileType  <-  "concept_ancestor.csv"
  cli::cli_progress_update()

  if (!file.exists(class_type_concept_ancestor_csv)) {


    #class_type_data <- load_rxclass_graph(class_types = class_type)
    node <- readr::read_csv(class_type_node_csv, col_types = readr::cols(.default = "c"), show_col_types = FALSE)
    edge <-
      readr::read_csv(class_type_edge_csv, col_types = readr::cols(.default = "c"), show_col_types = FALSE)

    if (nrow(edge) == 0) {

      readr::write_csv(
        x = edge,
        file = class_type_concept_ancestor_csv
      )

      return("")




    } else {



    edge <-
    edge %>%
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
      select(starts_with("parent_")) %>%
      rename_all(str_remove_all, "parent_") %>%
      dplyr::distinct()

    tmp_concept_ancestor3 <-
      tmp_concept_ancestor2 %>%
      # Adding an identifier per path to recurse later
      rowid_to_column("rowid")

    readr::write_csv(
      x = tmp_concept_ancestor3,
      file = tmp_concept_ancestor_csv
    )

    tmp_concept_ancestor4 <-
      pivot_longer(tmp_concept_ancestor3,
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
        group_by(rowid) %>%
        dplyr::filter(levels_of_separation == i) %>%
        ungroup() %>%
        dplyr::filter(!is.na(descendant_concept_code)) %>%
        transmute(rowid,
                  ancestor_concept_code = descendant_concept_code) %>%
        distinct() %>%
        left_join(tmp_concept_ancestor4,
                  by = "rowid") %>%
        dplyr::filter(!is.na(descendant_concept_code)) %>%
        distinct() %>%
        dplyr::mutate(levels_of_separation =
                        levels_of_separation-i) %>%
        dplyr::filter(levels_of_separation >= 0)

    }

    tmp_concept_ancestor6 <-
      bind_rows(tmp_concept_ancestor5) %>%
      select(-rowid) %>%
      distinct() %>%
      group_by(ancestor_concept_code,
               descendant_concept_code) %>%
      summarize(min_levels_of_separation = min(levels_of_separation),
                max_levels_of_separation = max(levels_of_separation),
                .groups = "drop") %>%
      ungroup()

    readr::write_csv(
      x =  tmp_concept_ancestor6,
      file = class_type_concept_ancestor_csv
    )

    unlink(tmp_ca_dir, recursive = TRUE)
    file.remove(tmp_concept_ancestor_csv)

    }


      }


    }


  }

