#' @importFrom httr GET content status_code
#' @importFrom cli cli_abort

get_rxnav_api_version <-
  function(expiration_hours = 4) {
    service_domain <- "https://rxnav.nlm.nih.gov"
    dirs <- "setupRxNormR"

    url <-
      paste0(
        service_domain,
        "/REST/version.json"
      )


    version_cache_file <-
    R.cache::findCache(
      key = list(url),
      dirs = dirs
    )



    expiration_secs <- expiration_hours*60*60

    if (is.null(version_cache_file)) {

      secs_since_cached <- expiration_secs

    } else {

      secs_since_cached <-
      lubridate::int_length(
        lubridate::interval(end = Sys.time(), start = file.info(version_cache_file)$ctime))


    }

    if (secs_since_cached >= expiration_secs) {

    Sys.sleep(3)
    ver_resp <-
      httr::GET(url)

    if (httr::status_code(ver_resp) != 200) {
      cli::cli_abort("API call to {.url {url}} returned Status Code {status_code(ver_resp)}.")
    }

    R.cache::saveCache(
      key = list(url),
      dirs = dirs,
      object = httr::content(ver_resp))


    }

    R.cache::loadCache(
      key = list(url),
      dirs = dirs
    )

  }


#' @importFrom httr status_code
#' @importFrom cli cli_abort

abort_on_api_error <-
  function(response) {
    if (httr::status_code(x = response) != 200) {
      cli::cli_abort(
        "Status code '{httr::status_code(response)}'."
      )
    }
  }
