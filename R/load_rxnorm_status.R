load_rxnorm_status <-
  function(
    rxnorm_statuses =
      c(
        "Active",
        "Remapped",
        "Obsolete",
        "Quantified",
        "NotCurrent"
      ),
    prior_version = NULL,
    prior_api_version = "3.1.174") {


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    } else {

      version_key <-
        list(version = prior_version,
             apiVersion = prior_api_version)

    }


    collect_rxnorm_status(rxnorm_statuses =
                         rxnorm_statuses,
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
        rxnorm = file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxNorm API"),
        all_concepts_by_status = file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxNorm API", "All Concepts By Status"))


    dirs_ls <-
      full_path_ls %>%
      purrr::map(
        function(x)
          stringr::str_remove_all(
            string = x,
            pattern = R.cache::getCacheRootPath()) %>%
          stringr::str_remove_all(pattern = "^[/]{1}")
        )
    dirs <- dirs_ls$all_concepts_by_status


    cli::cli_text(
      "[{as.character(Sys.time())}] {.emph {'Loading...'}}"
    )

    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.strong {rxnorm_status}} ",
        "({cli::pb_current}/{cli::pb_total})  ETA:{time_remaining}  Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Collected {cli::pb_total} statuses ",
        "in {cli::pb_elapsed}."
      ),
      total = length(rxnorm_statuses),
      clear = FALSE
    )

    i <- 0
    out <-
      vector(mode = "list",
             length = length(rxnorm_statuses))
    names(out) <- rxnorm_statuses
    for (rxnorm_status in rxnorm_statuses) {
      i <- i + 1
      time_remaining <- 3*(length(rxnorm_statuses)-i)
      time_remaining <- sprintf("%s secs", time_remaining)
      cli::cli_progress_update()

      url <-
        glue::glue("https://rxnav.nlm.nih.gov/REST/allstatus.json?status={rxnorm_status}")

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
          httr::GET(url)

        abort_on_api_error(resp)


        status_content <-
          httr::content(
            x = resp,
            as = "parsed"
          )[[1]][[1]] %>%
          purrr::transpose() %>%
          purrr::map(unlist) %>%
          tibble::as_tibble() %>%
          dplyr::transmute(
            rxcui,
            code = rxcui,
            str = name,
            tty,
            status = rxnorm_status
          )

        R.cache::saveCache(
          dirs = dirs,
          key = key,
          object = status_content
        )

        results <-
          R.cache::loadCache(
            dirs = dirs,
            key = key
          )
      }


      out[[rxnorm_status]] <-
        results


    }

    out



  }
