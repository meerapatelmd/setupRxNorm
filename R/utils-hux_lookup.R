
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
