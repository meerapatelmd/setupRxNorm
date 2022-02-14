#' @title
#' Collect RxClass Graph Data
#' @param class_types Vector of desired classTypes. This vector is also
#' in the order of the API calls will be made. Can be one or more of the following:
#' "MESHPA", "EPC", "MOA", "PE", "PK", "DISEASE", "DISPOS", "CHEM",
#' "SCHEDULE", "STRUCT", "TC", "VA", "ATC1-4".
#' @import tidyverse
#' @import cli
#' @import httr
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
    dirs        <- file.path("setupRxNorm", version_key$version, "RxClass")

    # If the version folder was not present in the cache, it means that
    # this is a brand new version
    version_dir <- file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version)
    if (!dir.exists(version_dir)) {
      pkg_dir <- file.path(R.cache::getCacheRootPath(), "setupRxNorm")
      cached_versions <-
      list.dirs(path = pkg_dir,
                full.names = FALSE,
                recursive = FALSE)

      cli::cli_alert_info(
        "New version {.emph {version_key$version}} is available at {.url {service_domain}}.")
      cli::cli_text(
        "Prior versions include:")
      names(cached_versions) <- "*"
      cli::cli_bullets(cached_versions)

    }

    http_request <- "/REST/rxclass/allClasses.json"
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

    class_df <-
      R.cache::loadCache(
        dirs = dirs,
        key = key
      )


    if (is.null(class_df)) {
      resp <-
        GET(url = url)


      abort_on_api_error(response = resp)

      class_df <-
        content(resp) %>%
        pluck("rxclassMinConceptList") %>%
        pluck("rxclassMinConcept") %>%
        map(as_tibble_row) %>%
        bind_rows()

      R.cache::saveCache(
        dirs = dirs,
        key = key,
        object = class_df
      )

      class_df <-
        R.cache::loadCache(
          dirs = dirs,
          key = key
        )
    }

    class_df <-
      class_df %>%
      dplyr::filter(classType %in% class_types) %>%
      mutate(
        classType =
          factor(classType, levels = class_types)) %>%
      arrange(classType) %>%
      mutate(classType = as.character(classType))


    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {pb_spin} {.strong {classType}}: {classId} {className} ",
        "({pb_current}/{pb_total})\tETA:{time_remaining}\tElapsed:{pb_elapsed}"
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

      classId      <- class_df$classId[kk]
      className    <- class_df$className[kk]
      classType    <- class_df$classType[kk]
      time_remaining <- as.character(lubridate::duration(seconds = (grand_total_calls-kk)*3))

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
          dirs = dirs,
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


        if (length(output0) == 1) {
          R.cache::saveCache(
            dirs = dirs,
            key = key,
            object = list(
              NODE = output0[[1]],
              EDGE = NULL
            )
          )
        } else if (length(output0) == 2) {

            R.cache::saveCache(
              dirs = dirs,
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




load_rxclass_graph <-
  function(class_types) {

    collect_rxclass_graph(class_types = class_types)

    service_domain <- "https://rxnav.nlm.nih.gov"

    version_key <- get_rxnav_api_version()
    dirs        <- file.path("setupRxNorm", version_key$version, "RxClass")

    http_request <- "/REST/rxclass/allClasses.json"
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

    class_df <-
      R.cache::loadCache(
        dirs = dirs,
        key = key
      )

    class_df <-
      class_df %>%
      dplyr::filter(classType %in% class_types) %>%
      mutate(
        classType =
          factor(classType, levels = class_types)) %>%
      arrange(classType) %>%
      mutate(classType = as.character(classType))


    output <- list()
    for (kk in 1:nrow(class_df)) {

      classId <- class_df$classId[kk]
      className <- class_df$className[kk]
      classType <- class_df$classType[kk]
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

      output[[kk]] <-
        R.cache::loadCache(
          dirs = dirs,
          key = key
        )

    }

    names(output) <-
      class_df$classId


    output


  }


