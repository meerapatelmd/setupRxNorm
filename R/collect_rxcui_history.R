collect_rxcui_history <-
  function(
    rxcuis,
    prior_version = NULL,
    prior_api_version = "3.1.174") {


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    } else {

      version_key <-
        list(version = prior_version,
             apiVersion = prior_api_version)

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
        rxnorm = file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxNorm API"),
        rxcui_history_status = file.path(R.cache::getCacheRootPath(), "setupRxNorm", version_key$version, "RxNorm API", "RxCUI History Status"))


    dirs_ls <-
      full_path_ls %>%
      purrr::map(
        function(x)
          stringr::str_remove_all(
            string = x,
            pattern = R.cache::getCacheRootPath()) %>%
          stringr::str_remove_all(pattern = "^[/]{1}")
        )

    dirs <- dirs_ls$rxcui_history_status


    cli::cli_text(
      "[{as.character(Sys.time())}] {.emph {'Collecting RxCUI Data...'}}"
    )

    cli::cli_progress_bar(
      format = paste0(
        "[{as.character(Sys.time())}] {.var RXCUI} {.emph {rxcui}} ",
        "({cli::pb_current}/{cli::pb_total})  ETA:{time_remaining}  Elapsed:{cli::pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {cli::col_green(cli::symbol$tick)} Collected data for {cli::pb_total} RxCUIs ",
        "in {cli::pb_elapsed}."
      ),
      total = length(rxcuis),
      clear = FALSE
    )


    i <- 0
    for (rxcui in rxcuis) {

      i <- i+1
      time_remaining <- 3*(length(rxcuis)-i)
      time_remaining <- as.character(lubridate::duration(seconds = time_remaining))

      cli::cli_progress_update()

      url <-
        glue::glue("https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/historystatus.json")

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

      rxcui_out <-
        httr::content(resp,
                as = "parsed")

      if (!is.null(rxcui_out)) {
        rxcui_out <-
          rxcui_out$rxcuiStatusHistory$derivedConcepts$remappedConcept %>%
          purrr::map(unlist) %>%
          purrr::map(tibble::as_tibble_row) %>%
          dplyr::bind_rows()
      } else {
        rxcui_out <-
          tibble::tribble(
            ~remappedRxCui,
            ~remappedName,
            ~remappedTTY
          )
      }


      R.cache::saveCache(
        dirs = dirs,
        key = key,
        object = rxcui_out
      )

    }



    }




  }
