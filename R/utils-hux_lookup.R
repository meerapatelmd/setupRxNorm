
print_lookup <-
  function(lookup) {
  lookup_hx <-
  lookup %>%
    huxtable::hux()

  lookup_hx %>%
    huxtable::theme_basic() %>%
    huxtable::set_all_borders() %>%
    huxtable::set_align(value = "centre") %>%
    huxtable::set_all_padding(value = "10px") %>%
    huxtable::print_screen(colnames = FALSE)

}


print_df <-
  function(df,
           highlight_row) {

    df <-
      huxtable::hux(df)

    if (highlight_row == 1) {
    df %>%
      huxtable::theme_basic() %>%
      huxtable::set_all_borders() %>%
      huxtable::set_align(value = "centre") %>%
      huxtable::set_all_padding(value = "10px") %>%
      huxtable::set_background_color(row = highlight_row+1,
                                     value = "yellow") %>%
      huxtable::print_screen(colnames = FALSE)


    } else {

      df %>%
        huxtable::theme_basic() %>%
        huxtable::set_all_borders() %>%
        huxtable::set_align(value = "centre") %>%
        huxtable::set_all_padding(value = "10px") %>%
        huxtable::set_background_color(row = 2:highlight_row,
                                       value = "black") %>%
        huxtable::set_background_color(row = highlight_row+1,
                                       value = "yellow") %>%
        huxtable::print_screen(colnames = FALSE)


    }



  }
