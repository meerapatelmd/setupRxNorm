extract_rxnorm_status <-
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

    rxnorm_status_data <-
      load_rxnorm_status(
        rxnorm_statuses = rxnorm_statuses,
        prior_version = version_key$version,
        prior_api_version = version_key$apiVersion
      )


    path_vctr <-
      c(here::here(),
        "inst",
        "RxNorm API",
        version_key$version,
        "extracted",
        "status")

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
                "status.csv")

    if (!file.exists(file)) {


      readr::write_csv(
        x = dplyr::bind_rows(rxnorm_status_data),
        file = file
      )



    }



  }

