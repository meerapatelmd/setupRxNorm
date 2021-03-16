
<!-- README.md is generated from README.Rmd. Please edit that file -->

# setupRxNorm

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![CRAN
status](https://www.r-pkg.org/badges/version/setupRxNorm)](https://CRAN.R-project.org/package=setupRxNorm)
<!-- badges: end -->

The goal of setupRxNorm is to instantiate the RxNorm Monthly Release on
Postgres.

## Installation

You can install setupRxNorm from [GitHub](https://GitHub.com) with:

``` r
library(devtools)
install_github("meerapatelmd/setupRxNorm")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(setupRxNorm)
run_setup(conn = conn, 
          rrf_path = "~/Desktop/RxNorm_full_03012021/rrf", 
          log_release_date = "2021-03-01")
```
