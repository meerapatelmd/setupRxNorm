#' @importFrom lubridate duration

calculate_time_remaining <-
  function (iteration, total_iterations, time_value_per_iteration,
            time_unit_per_iteration = c("seconds", "minutes", "hours",
                                        "days", "weeks", "months", "years", "milliseconds", "microseconds",
                                        "nanoseconds", "picoseconds"))
  {
    time_unit_per_iteration <- match.arg(arg = time_unit_per_iteration,
                                         choices = c("seconds", "minutes", "hours", "days", "weeks",
                                                     "months", "years", "milliseconds", "microseconds",
                                                     "nanoseconds", "picoseconds"), several.ok = F)
    lubridate::duration(num = time_value_per_iteration * ((total_iterations -
                                                             iteration)), units = time_unit_per_iteration)
  }
