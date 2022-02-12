library(tidyverse)
library(cli)
library(httr)



collect_rxclass_data <-
  function() {
    service_domain <- "https://rxnav.nlm.nih.gov"
    url <-
      paste0(
        service_domain,
        "/REST/version.json"
      )
    Sys.sleep(3)
    ver_resp <-
      GET(url)

    if (status_code(ver_resp) != 200) {
      cli::cli_abort("API call to {.url {url}} returned Status Code {status_code(ver_resp)}.")
    }

    version_key <- content(ver_resp)

    http_request <- "/REST/rxclass/allClasses.json"
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
        dirs = "setupRxNorm",
        key = key
      )


    if (is.null(class_df)) {
      resp <-
        GET(url = url)

      if (status_code(ver_resp) != 200) {
        cli::cli_abort("API call to {.url {url}} returned Status Code {status_code(resp)}.")
      }

      class_df <-
        content(resp) %>%
        pluck("rxclassMinConceptList") %>%
        pluck("rxclassMinConcept") %>%
        map(as_tibble_row) %>%
        bind_rows()

      R.cache::saveCache(
        dirs = "setupRxNorm",
        key = key,
        object = class_df
      )

      class_df <-
        R.cache::loadCache(
          dirs = "setupRxNorm",
          key = key
        )
    }



    # Remove ATC because redundant with OMOP
    class_df <-
      class_df %>%
      mutate(
        classType =
          factor(classType,
            levels =
              c(
                "MESHPA",
                "EPC",
                "MOA",
                "PE",
                "PK",
                "DISEASE",
                "DISPOS",
                "CHEM",
                "SCHEDULE",
                "STRUCT",
                "TC",
                "VA",
                "ATC1-4"
              )
          )
      ) %>%
      arrange(classType) %>%
      mutate(classType = as.character(classType))


    cli::cli_progress_bar(
      format = paste0(
        "{pb_spin} {.strong {classType}} {classId} {className} ",
        "[{pb_current}/{pb_total}]   Elapsed:{pb_elapsed}"
      ),
      format_done = paste0(
        "[{as.character(Sys.time())}] {col_green(symbol$tick)} Downloaded {pb_total} files ",
        "in {pb_elapsed}."
      ),
      total = nrow(class_df),
      clear = FALSE
    )

    for (kk in 1:nrow(class_df)) {
      cli::cli_progress_update()
      Sys.sleep(0.1)

      classId <- class_df$classId[kk]
      className <- class_df$className[kk]
      classType <- class_df$classType[kk]
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

      results <-
        R.cache::loadCache(
          dirs = "setupRxNorm",
          key = key
        )


      if (is.null(results)) {
        Sys.sleep(3)
        resp <-
          GET(url = url)

        output0 <-
          content(resp) %>%
          pluck("rxclassGraph") %>%
          map(function(x) map(x, as_tibble_row)) %>%
          map(bind_rows)


        if (length(output0) == 1) {
          R.cache::saveCache(
            dirs = "setupRxNorm",
            key = key,
            object = list(
              CONCEPT = output0[[1]],
              CONCEPT_RELATIONSHIP = NULL,
              CONCEPT_ANCESTOR = NULL
            )
          )
        } else if (length(output0) == 2) {
          output0 <-
            output0 %>%
            set_names(nm = c("CONCEPT", "CONCEPT_RELATIONSHIP"))

          # Creating CONCEPT_ANCESTOR-esque table
          CONCEPT_RELATIONSHIP <-
            output0$CONCEPT_RELATIONSHIP

          # Level 0
          CONCEPT_ANCESTOR0 <- list()
          for (i in 1:nrow(CONCEPT_RELATIONSHIP)) {
            parent <- CONCEPT_RELATIONSHIP$classId1[i]
            child <- CONCEPT_RELATIONSHIP$classId2[i]

            # Parent of the Level,
            # Child of the Level,
            # Child again for the next level
            x <-
              tibble(
                parent = parent,
                child = child,
                child_as_parent = child
              )

            colnames(x) <-
              c(
                sprintf("parent_%s", i - 1),
                sprintf("child_%s", i - 1),
                sprintf("parent_%s", i)
              )

            CONCEPT_ANCESTOR0[[length(CONCEPT_ANCESTOR0) + 1]] <- x
          }

          silent_join <-
            function(x,
                     y,
                     by = NULL,
                     copy = FALSE,
                     suffix = c(".x", ".y"),
                     ...,
                     keep = FALSE) {
              suppressMessages(
                inner_join(
                  x = x,
                  y = y,
                  by = by,
                  copy = copy,
                  suffix = suffix,
                  ...,
                  keep = keep
                )
              )
            }

          CONCEPT_ANCESTOR1 <-
            CONCEPT_ANCESTOR0 %>%
            reduce(silent_join) %>%
            select(starts_with("parent")) %>%
            rename_all(str_remove_all, "parent_") %>%
            pivot_longer(
              cols = everything(),
              names_to = "level_of_separation",
              values_to = "ancestor_code"
            )


          if (nrow(CONCEPT_ANCESTOR1) > 0) {
            output <- list()
            for (j in 1:nrow(CONCEPT_ANCESTOR1)) {
              ancestor_code <-
                CONCEPT_ANCESTOR1$ancestor_code[j]

              if (j == 1) {
                root_df <-
                  bind_cols(
                    tibble(
                      ancestor_code = ancestor_code
                    ),
                    CONCEPT_ANCESTOR1 %>%
                      transmute(
                        descendant_code = ancestor_code,
                        level_of_separation = as.integer(level_of_separation)
                      )
                  )

                output[[j]] <-
                  root_df
              } else {
                output[[j]] <-
                  root_df %>%
                  dplyr::slice(j:nrow(CONCEPT_ANCESTOR1)) %>%
                  select(descendant_code) %>%
                  transmute(
                    ancestor_code = ancestor_code,
                    descendant_code = descendant_code,
                    level_of_separation = (1:n()) - 1
                  )
              }
            }

            CONCEPT_ANCESTOR <-
              bind_rows(output) %>%
              distinct()

            R.cache::saveCache(
              dirs = "setupRxNorm",
              key = key,
              object = list(
                CONCEPT = output0$CONCEPT,
                CONCEPT_RELATIONSHIP = output0$CONCEPT_RELATIONSHIP,
                CONCEPT_ANCESTOR = CONCEPT_ANCESTOR
              )
            )
          } else {
            R.cache::saveCache(
              dirs = "setupRxNorm",
              key = key,
              object = list(
                CONCEPT = output0$CONCEPT,
                CONCEPT_RELATIONSHIP = output0$CONCEPT_RELATIONSHIP,
                CONCEPT_ANCESTOR = "Error"
              )
            )
          }
        }
      }
    }
  }
