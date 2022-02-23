#' @title
#' Collect RxClass Graph Data
#' @param class_types Vector of desired classTypes. This vector is also
#' in the order of the API calls will be made. Can be one or more of the following:
#' "MESHPA", "EPC", "MOA", "PE", "PK", "DISEASE", "DISPOS", "CHEM",
#' "SCHEDULE", "STRUCT", "TC", "VA", "ATC1-4".
#' @import tidyverse
#' @import cli
#' @import httr
#' @import purrr
#' @rdname collect_rxclass_graph
#' @export
collect_rxclass_graph <-
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
      "DISPOS",
      "CHEM",
      "SCHEDULE",
      "STRUCT")) {

    service_domain <- "https://rxnav.nlm.nih.gov"

    version_key <- get_rxnav_api_version()


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
        "({cli::pb_current}/{cli::pb_total})  ETA:{time_remaining}  Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Collected {cli::pb_total} graphs ",
        "in {cli::pb_elapsed}."
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
        glue::glue("/REST/rxclass/classGraph.json?classId={classId}")

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
          content(resp) %>%
          pluck("rxclassGraph") %>%
          map(function(x) map(x, as_tibble_row)) %>%
          map(bind_rows)

        if (length(output0)==0) {

          R.cache::saveCache(
            dirs = dirs_kk,
            key = key,
            object = list(
              NODE = NULL,
              EDGE = NULL
            )
          )

        } else if (length(output0) == 1) {
          R.cache::saveCache(
            dirs = dirs_kk,
            key = key,
            object = list(
              NODE = output0[[1]],
              EDGE = NULL
            )
          )
        } else if (length(output0) == 2) {

            R.cache::saveCache(
              dirs = dirs_kk,
              key = key,
              object = list(
                NODE = output0[[1]],
                EDGE = output0[[2]]
              )
            )
          }
        }
      }
    }
