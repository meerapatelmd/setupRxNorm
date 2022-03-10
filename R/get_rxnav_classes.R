get_rxnav_classes <-
  function(class_types = c(
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
               "DISPOS"),
           prior_version = NULL,
           prior_api_version = "3.1.174") {

    version_key <-
      list(version = prior_version,
           apiVersion = prior_api_version)


    if (is.null(prior_version)) {

      version_key <- get_rxnav_api_version()

    }



  service_domain <- "https://rxnav.nlm.nih.gov"

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



# If the version folder was not present in the cache, it means that
# this is a brand new version
# ---
# setupRxNorm /
#     07-Feb-2022 /
#        MESHPA /
#        TC /
#        VA /
#        ...
if (!dir.exists(full_path_ls$version)) {
  pkg_dir <- full_path_ls$pkg
  cached_versions <-
    list.dirs(path = pkg_dir,
              full.names = FALSE,
              recursive = FALSE)

  if (length(cached_versions)>0) {

    cli::cli_alert_info(
      "New version {.emph {version_key$version}} is available at {.url {service_domain}}.")
    cli::cli_text(
      "Prior versions include:")
    names(cached_versions) <- "*"
    cli::cli_bullets(cached_versions)

  } else {

    cli::cli_alert_info(
      "First cache created with version {.emph {version_key$version}}.")


  }

}

http_request <-
  "/REST/rxclass/allClasses.json"

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
    dirs = dirs_ls$version,
    key = key
  )


if (is.null(class_df)) {
  resp <-
    httr::GET(url = url)


  abort_on_api_error(response = resp)

  class_df <-
    httr::content(resp) %>%
    purrr::pluck("rxclassMinConceptList") %>%
    purrr::pluck("rxclassMinConcept") %>%
    purrr::map(as_tibble_row) %>%
    dplyr::bind_rows()

  if (is.null(class_df)) {

    cli::cli_abort("Classes could not be collected.")

  }

  R.cache::saveCache(
    dirs = dirs_ls$version,
    key = key,
    object = class_df
  )

  class_df <-
    R.cache::loadCache(
      dirs = dirs_ls$version,
      key = key
    )
}


  class_df
  }
