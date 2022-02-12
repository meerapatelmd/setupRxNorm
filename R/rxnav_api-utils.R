get_rxnav_api_version <-
  function() {
    service_domain <- "https://rxnav.nlm.nih.gov"
    url <-
      paste0(
        service_domain,
        "/REST/version.json"
      )
    Sys.sleep(3)
    ver_resp <-
      httr::GET(url)

    if (status_code(ver_resp) != 200) {
      cli::cli_abort("API call to {.url {url}} returned Status Code {status_code(ver_resp)}.")
    }

    httr::content(ver_resp)
  }



abort_on_api_error <-
  function(response) {
    if (httr::status_code(x = response) != 200) {
      cli::cli_abort(
        "Status code '{httr::status_code(response)}'."
      )
    }
  }
