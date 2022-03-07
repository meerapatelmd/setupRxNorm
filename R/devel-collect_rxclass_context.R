#' @title
#' Collect RxClass Context Data
#' @description
#' Get the ancestors of a class specified by source and classId. The ancestors are returned as a graph of nodes and edges.The source, which restricts the results to a particular class type, should be specified when using a MeSH identifier because some MeSH concepts exist in more than one RxClass tree. getClassGraphBySource returns only one tree. To find all trees that contain a concept, use getClassContexts (`*_rxclass_context`).This resource returns an array of nodes (rxclassMinConceptItem) and an array of edges (rxclassEdge) that link the nodes.
#' @param class_types Vector of desired classTypes. This vector is also
#' in the order of the API calls will be made. Can be one or more of the following:
#' "MESHPA", "EPC", "MOA", "PE", "PK", "DISEASE", "DISPOS", "CHEM",
#' "SCHEDULE", "STRUCT", "TC", "VA", "ATC1-4".
#' @import tidyverse
#' @import cli
#' @import httr
#' @rdname collect_rxclass_context
#' @export
collect_rxclass_context <-
  function(class_types = c(
      "MESHPA",
      "EPC",
      "MOA",
      "PE",
      "PK",
      "TC",
      "VA",
      "DISEASE",
      "DISPOS",
      "CHEM",
      "SCHEDULE",
      "STRUCT")) {

    service_domain <- "https://rxnav.nlm.nih.gov"


    # If the version folder was not present in the cache, it means that
    # this is a brand new version
    # ---
    # setupRxNorm /
    #     07-Feb-2022 /
    #        MESHPA /
    #        TC /
    #        VA /
    #        ...
    full_path_ls <-
      list(
        pkg     = file.path(R.cache::getCacheRootPath(), "setupRxNorm"),
        version = file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version),
        rxclass = file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxClass API"),
        class_types =
          file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxClass API", class_types) %>%
          set_names(class_types) %>%
          as.list()
      )


    dirs_ls <-
      list(
        pkg     = file.path("setupRxNorm"),
        version = file.path("setupRxNorm", version_key$version),
        rxclass = file.path("setupRxNorm", version_key$version, "RxClass API"),
        class_types =
          file.path("setupRxNorm", version_key$version, "RxClass API", class_types) %>%
          set_names(class_types) %>%
          as.list()
      )


    class_df <- get_rxnav_classes()
    class_df <-
      class_df %>%
      dplyr::filter(classType %in% class_types) %>%
      mutate(
        classType =
          factor(classType, levels = class_types)) %>%
      arrange(classType) %>%
      mutate(classType = as.character(classType))

    cli::cli_text(
      "[{as.character(Sys.time())}] {.emph {'Collecting...'}}"
    )

    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {classType}}: {classId} {className} ",
        "({pb_current}/{pb_total})  ETA:{time_remaining}  Elapsed:{pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {col_green(symbol$tick)} Collected {pb_total} graphs ",
        "in {pb_elapsed}."
      ),
      total = nrow(class_df),
      clear = FALSE
    )

    # Total time it would take from scratch
    # 3 seconds * total calls that need to be made
    grand_total_calls <- nrow(class_df)

    time_remaining <- as.character(lubridate::duration(seconds = (grand_total_calls)*3))
    for (kk in 1:nrow(class_df)) {
      cli::cli_progress_update()
      # Sys.sleep(0.01)

      classId        <- class_df$classId[kk]
      className      <- class_df$className[kk]
      classType      <- class_df$classType[kk]
      time_remaining <- as.character(lubridate::duration(seconds = (grand_total_calls-kk)*3))
      dirs_kk        <- dirs_ls$class_types[[classType]]
      http_request <-
        glue::glue("/REST/rxclass/classContext.json?classId={classId}")

      url <-
        paste0(
          service_domain,
          http_request
        )

      key <-
        list(
          version_key,
          url
        )

      results <-
        R.cache::loadCache(
          dirs = dirs_kk,
          key = key
        )


      if (is.null(results)) {
        Sys.sleep(3)
        resp <-
          GET(url = url)

        output0 <-
          content(resp, as = "text", encoding = "UTF-8") %>%
          jsonlite::fromJSON() %>%
          pluck(1, 1, 1) %>%
          map(rowid_to_column)

        names(output0) <-
          as.character(1:length(output0))

        output0 <-
          bind_rows(output0,
                    .id = "contextId")

        R.cache::saveCache(
          dirs = dirs_kk,
          key = key,
          object = output0
        )
      }
    }
}
