drop_dir <-
  function(dir) {

    unlink(dir,
           recursive = TRUE)
    unlink(dir)


  }
