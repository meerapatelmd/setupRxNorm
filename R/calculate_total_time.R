calculate_total_time  <-
  function (total_iterations)
  {
    time_unit_per_iteration <- "seconds"
    lubridate::duration(num = 3*total_iterations,
                        units = time_unit_per_iteration)
  }
