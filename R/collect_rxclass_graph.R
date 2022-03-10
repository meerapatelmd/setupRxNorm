#' @title
#' Collect RxClass Graph Data
#' @param class_types Vector of desired classTypes. This vector is also
#' in the order of the API calls will be made. Can be one or more of the following:
#' "MESHPA", "EPC", "MOA", "PE", "PK", "DISEASE", "DISPOS", "CHEM",
#' "SCHEDULE", "STRUCT", "TC", "VA", "ATC1-4".
#' @rdname collect_rxclass_graph
#' @importFrom R.cache getCacheRootPath loadCache saveCache
#' @importFrom dplyr filter mutate arrange bind_rows
#' @importFrom cli cli_text cli_progress_bar cli_progress_update
#' @importFrom lubridate duration
#' @importFrom glue glue
#' @importFrom httr GET content
#' @importFrom purrr pluck map
#' @importFrom tibble as_tibble_row
#' @export
#' @family Collect functions
#' @family RxClass Graph functions

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
      "STRUCT"),
    prior_version = NULL,
    prior_api_version = "3.1.174") {


    version_key <-
      list(version = prior_version,
           apiVersion = prior_api_version)


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    }

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
          purrr::set_names(class_types) %>%
          as.list()
      )


    dirs_ls <-
      list(
        pkg     = file.path("setupRxNorm"),
        version = file.path("setupRxNorm", version_key$version),
        rxclass = file.path("setupRxNorm", version_key$version, "RxClass API"),
        class_types =
          file.path("setupRxNorm", version_key$version, "RxClass API", class_types) %>%
          purrr::set_names(class_types) %>%
          as.list()
      )


    class_df <- get_rxnav_classes(
      prior_version = version_key$version,
      prior_api_version = version_key$apiVersion)
    class_df <-
      class_df %>%
      dplyr::filter(classType %in% class_types) %>%
      dplyr::mutate(
        classType =
          factor(classType, levels = class_types)) %>%
      dplyr::arrange(classType) %>%
      dplyr::mutate(classType = as.character(classType))

    cli::cli_text(
      "[{as.character(Sys.time())}] {.emph {'Collecting hierachies (graphs)...'}}"
    )

    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {classType}}: {classId} {className} ",
        "({cli::pb_current}/{cli::pb_total})  ETA:{time_remaining}  Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Collected {cli::pb_total} {classType} graphs ",
        "in {cli::pb_elapsed}."
      ),
      total = nrow(class_df),
      clear = TRUE
    )

    for (kk in 1:nrow(class_df)) {

      classId        <- class_df$classId[kk]
      className      <- class_df$className[kk]
      classType      <- class_df$classType[kk]
      time_remaining <- as.character(calculate_time_remaining(total_iterations = nrow(class_df),
                                                              iteration = kk,
                                                              time_value_per_iteration = 3,
                                                              time_unit_per_iteration = "seconds"))
      dirs_kk        <- dirs_ls$class_types[[classType]]
      http_request <-
        glue::glue("/REST/rxclass/classGraph.json?classId={classId}")

      url <-
        paste0(
          service_domain,
          http_request
        )
      cli::cli_progress_update()

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

        httr::set_config(httr::config(http_version = 0))

        Sys.sleep(3)
        resp <-
          httr::GET(url = url)

        output0 <-
          httr::content(resp,
                        encoding = "UTF-8") %>%
          purrr::pluck("rxclassGraph") %>%
          purrr::map(function(x) purrr::map(x, tibble::as_tibble_row)) %>%
          purrr::map(dplyr::bind_rows)

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
