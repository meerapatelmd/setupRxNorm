load_rxclass_graph <-
  function(class_types,
           prior_version = NULL,
           prior_api_version = "3.1.174") {


    version_key <-
      list(version = prior_version,
           apiVersion = prior_api_version)


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    }

    class_types <-
    match.arg(arg = class_types,
              choices =
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
              several.ok = TRUE)

    collect_rxclass_graph(class_types = class_types,
                          prior_version = version_key$version,
                          prior_api_version = version_key$apiVersion)

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


    class_df <- get_rxnav_classes(prior_version = version_key$version,
                                  prior_api_version = version_key$apiVersion)
    class_df <-
      class_df %>%
      dplyr::filter(classType %in% class_types) %>%
      mutate(
        classType =
          factor(classType, levels = class_types)) %>%
      arrange(classType) %>%
      mutate(classType = as.character(classType))

    cli::cli_text(
      "[{as.character(Sys.time())}] {.emph {'Loading RxClass Graphs...'}}"
    )

    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {classType}}: {classId} {className} ",
        "({cli::pb_current}/{cli::pb_total})  ETA:{time_remaining}  Elapsed:{cli::pb_elapsed}\n",
        "[{as.character(Sys.time())}] {.url {url}}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(symbol$tick)} Loaded {cli::pb_total} RxClass graphs ",
        "in {cli::pb_elapsed}."
      ),
      total = nrow(class_df),
      clear = FALSE
    )


    output <- list()
    for (kk in 1:nrow(class_df)) {

      classId <- class_df$classId[kk]
      className <- class_df$className[kk]
      classType <- class_df$classType[kk]
      dirs_kk        <- dirs_ls$class_types[[classType]]
      time_remaining <- as.character(lubridate::duration(seconds = ((nrow(class_df)-kk))*3))

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

      output[[kk]] <-
        R.cache::loadCache(
          dirs = dirs_kk,
          key = key
        )

    }

    names(output) <-
      class_df$classId

    output %>%
      purrr::transpose() %>%
      purrr::map(bind_rows)




  }
