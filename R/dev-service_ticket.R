get_service_ticket <-
  function(store_creds_at = "~/Desktop") {

    tgt_file_path <- file.path(store_creds_at, ".umls_api_tgt.txt")
    service_ticket_file_path <- file.path(store_creds_at, ".umls_api_service_ticket.txt")

    if (file.exists(tgt_file_path)) {

      if (difftime(Sys.time(), file.info(tgt_file_path)$mtime, units = "hours")  > 8) {

        proceed <- TRUE

      } else {

        proceed <- FALSE

      }
    } else {
      proceed <- TRUE
    }


    if (proceed) {

      auth_response <-
        httr::POST(
          url = "https://utslogin.nlm.nih.gov/cas/v1/api-key",
          body = list(apikey = Sys.getenv("UMLS_API_KEY")),
          encode = "form"
        )


      TGT <-
        httr::content(auth_response, type = "text/html", encoding = "UTF-8") %>%
        rvest::html_nodes("form") %>%
        rvest::html_attr("action")

      cat(TGT,
          sep = "\n",
          file = tgt_file_path)

      tgt_response <-
        httr::POST(
          url = TGT,
          body = list(service = "http://umlsks.nlm.nih.gov"),
          encode = "form")

      if (httr::status_code(tgt_response) != 200) {
        stop(
          sprintf(
            "API request for Service Ticket failed",
            httr::status_code(auth_response)
          ),
          call. = FALSE
        )
      }


      service_ticket <-
        tgt_response %>%
        httr::content(type = "text/html", encoding = "UTF-8") %>%
        rvest::html_text()

      cat(service_ticket,
          sep = "\n",
          file = service_ticket_file_path)

    } else {

      TGT <- readLines(tgt_file_path)


      if (file.exists(service_ticket_file_path)) {
        if (Sys.time() - file.info(service_ticket_file_path)$mtime >= 4) {
          proceed2 <- TRUE
        } else {
          proceed2 <- FALSE
        }
      } else {
        proceed2 <- TRUE
      }

      if (proceed2) {

        tgt_response <-
          httr::POST(
            url = TGT,
            body = list(service = "http://umlsks.nlm.nih.gov"),
            encode = "form")

        if (httr::status_code(tgt_response) != 200) {
          stop(
            sprintf(
              "API request for Service Ticket failed",
              httr::status_code(auth_response)
            ),
            call. = FALSE
          )
        }

        service_ticket <-
          tgt_response %>%
          httr::content(type = "text/html", encoding = "UTF-8") %>%
          rvest::html_text()

        cat(service_ticket,
            sep = "\n",
            file = service_ticket_file_path)
      } else {
        service_ticket <- readLines(service_ticket_file_path)
      }

    }

    file.remove(service_ticket_file_path)
    service_ticket
  }
