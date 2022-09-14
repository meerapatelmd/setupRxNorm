extract_rxcui_history <-
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

    path_vctr <-
      c(here::here(),
        "dev",
        "RxNorm API",
        version_key$version,
        "extracted",
        "history")

    for (i in 1:length(path_vctr)) {

      write_dir <-
        paste(path_vctr[1:i],
              collapse = .Platform$file.sep)


      if (!dir.exists(write_dir)) {


        dir.create(write_dir)

      }


    }

    file <-
      file.path(write_dir,
                "history.csv")

    if (!file.exists(file)) {


    rxnorm_api_version <-
      sprintf("%s %s", version_key$version, version_key$apiVersion)

    rxcui_history_data <-
      load_rxcui_history(
        rxcuis = rxcuis,
        prior_version = version_key$version,
        prior_api_version = version_key$apiVersion
      )

    rxcui_history_data2 <-
    rxcui_history_data %>%
      dplyr::bind_rows() %>%
      dplyr::transmute(
        input_rxcui = inputRxCui,
        output_code = remappedRxCui,
        output_str = remappedName,
        output_tty = remappedTTY
      ) %>%
      dplyr::group_by(input_rxcui) %>%
      dplyr::arrange(as.integer(output_code),
                     .by_group = TRUE
      ) %>%
      dplyr::mutate(
        output_code_cardinality =
          length(unique(output_code)),
        output_codes =
          paste(unique(output_code),
                collapse = "|"
          )
      ) %>%
      dplyr::filter(dplyr::row_number() == 1) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        output_source = "RxNav REST API",
        output_source_version = rxnorm_api_version
      )


    path_vctr <-
      c(here::here(),
        "dev",
        "RxNorm API",
        version_key$version,
        "extracted",
        "history")

    for (i in 1:length(path_vctr)) {

      write_dir <-
        paste(path_vctr[1:i],
              collapse = .Platform$file.sep)


      if (!dir.exists(write_dir)) {


        dir.create(write_dir)

      }


    }



      readr::write_csv(
        x = rxcui_history_data2,
        file = file)


    }




  }
