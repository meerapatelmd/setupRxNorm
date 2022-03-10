cli_file_missing <-
  function(file_path) {

    console_file <-
    stringr::str_replace(file_path,
                         pattern = "(^.*?/)(inst.*?)",
                         replacement = "\\2")


   cli::cli_text("[{as.character(Sys.time())}] {cli::col_red(cli::style_bold(cli::symbol$cross))} {.file {console_file}} ")


  }


cli_missing_file_written <-
  function(file_path) {

console_file <-
  stringr::str_replace(file_path,
                       pattern = "(^.*?/)(inst.*?)",
                       replacement = "\\2")


cli::cli_text("[{as.character(Sys.time())}]   {cli::style_bold(cli::symbol$arrow_right)} {cli::col_green(cli::style_bold(cli::symbol$tick))} {.file {console_file}} ")


}


cli_file_exists <-
  function(file_path) {

    console_file <-
      stringr::str_replace(file_path,
                           pattern = "(^.*?/)(inst.*?)",
                           replacement = "\\2")


    cli::cli_text("[{as.character(Sys.time())}] {cli::style_bold(cli::symbol$checkbox_on)} {.file {console_file}} ")


  }

