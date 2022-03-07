qa_rxclass_concept_classes <-
function(prior_version = NULL,
         prior_api_version = "3.1.174") {

  if (is.null(prior_version)) {

    version_key <- get_rxnav_api_version()

  } else {

    version_key <-
      list(version = prior_version,
           apiVersion = prior_api_version)

  }

  member_concept_classes_data <<-
    read_members_concept_classes_csvs(
      prior_version = version_key$version,
      prior_api_version = version_key$apiVersion
    )


  graph_concept_classes_data <<-
    read_graph_concept_csvs(
      prior_version = version_key$version,
      prior_api_version = version_key$apiVersion
    )

  reconciled_data <-
    dplyr::full_join(
      member_concept_classes_data,
      graph_concept_classes_data,
      by = c("concept_code", "standard_concept",  "class_type"),
      keep = TRUE,
      suffix = c(".members", ".graph")
    )


  concept_classes_orphans <<-
    reconciled_data %>%
    dplyr::filter_at(dplyr::vars(ends_with(".graph")),
                     dplyr::all_vars(is.na(.))) %>%
    dplyr::transmute(
      concept_code = concept_code.members,
      concept_name = '(Missing)',
      class_type   = class_type.members,
      standard_concept = standard_concept.members) %>%
    dplyr::left_join(
      classtype_lookup %>%
        dplyr::transmute(
          class_type = classType,
          vocabulary_id = dplyr::coalesce(omop_vocabulary_id, custom_vocabulary_id)
        ),
      by = "class_type") %>%
    dplyr::distinct()

  cli::cli_text(
    "[{as.character(Sys.time())}] {nrow(concept_classes_orphans)} classes in RxClass Members, but not found in RxClass Graph:"
  )

  print_lookup(concept_classes_orphans %>%
                 dplyr::count(vocabulary_id, class_type))


  cli::cli_progress_bar(
    format = paste0(
      "[{as.character(Sys.time())}] {.strong {classType} ({vocabulary_id}) code}: {orphanClassId} ",
      "({cli::pb_current}/{cli::pb_total})  Elapsed:{cli::pb_elapsed}"
    ),
    format_done = paste0(
      "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Collected {cli::pb_total} class details ",
      "in {cli::pb_elapsed}."
    ),
    total = nrow(concept_classes_orphans),
    clear = FALSE
  )

  orphanClassDetails <- list()
  dirs <- file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxClass API", "Orphan Classes")
  for (i in 1:nrow(concept_classes_orphans)) {

    orphanClassId <- concept_classes_orphans$concept_code[i]
    classType     <- concept_classes_orphans$class_type[i]
    vocabulary_id <- concept_classes_orphans$vocabulary_id[i]
    cli::cli_progress_update()

    url <-
      glue::glue(
        "https://rxnav.nlm.nih.gov/REST/rxclass/class/byId.json?classId={orphanClassId}"
      )

    key <-
      list(
        version_key,
        url
      )

    results <-
      R.cache::loadCache(
        dirs = dirs,
        key = key
      )

    if (is.null(results)) {

      Sys.sleep(3)
      resp <-
        httr::GET(url = url)

      if (status_code(resp) != 200) {

        stop()

      }

      results0 <-
        httr::content(resp)[["rxclassMinConceptList"]][["rxclassMinConcept"]][[1]]

      R.cache::saveCache(
        dirs = dirs,
        key  = key,
        object = results0
      )

      results <-
        R.cache::loadCache(
          dirs = dirs,
          key = key
        )

    }

    orphanClassDetails[[length(orphanClassDetails)+1]] <-
      results

    names(orphanClassDetails)[length(orphanClassDetails)] <-
      orphanClassId

  }

  concept_classes_orphans_b <-
    orphanClassDetails %>%
    purrr::map(unlist) %>%
    purrr::map(as_tibble_row) %>%
    dplyr::bind_rows() %>%
    dplyr::transmute(
      concept_code = classId,
      class_type = classType,
      concept_name = className) %>%
    dplyr::distinct()

  dplyr::left_join(
    concept_classes_orphans,
    concept_classes_orphans_b,
    keep = TRUE,
    suffix = c(".old", ".new"),
    by = c("concept_code", "class_type")) %>%
    dplyr::select(
      concept_code = concept_code.new,
      concept_name = concept_name.new,
      class_type   = class_type.new,
      standard_concept,
      vocabulary_id) %>%
    dplyr::distinct()


  }
