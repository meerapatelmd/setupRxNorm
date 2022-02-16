load_rxclass_graph <-
  function(class_types) {

    collect_rxclass_graph(class_types = class_types)

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


    output <- list()
    for (kk in 1:nrow(class_df)) {

      classId <- class_df$classId[kk]
      className <- class_df$className[kk]
      classType <- class_df$classType[kk]
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

      output[[kk]] <-
        R.cache::loadCache(
          dirs = dirs_kk,
          key = key
        )

    }

    names(output) <-
      class_df$classId

    output %>%
      transpose() %>%
      map(bind_rows)




  }
