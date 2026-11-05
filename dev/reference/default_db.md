# The name of the default database

If the queue database is not specified explicitly, then `liteq` uses
this file. Its location is determined via the `rappdirs` package, see
[`rappdirs::user_data_dir()`](https://rappdirs.r-lib.org/reference/user_data_dir.html).

## Usage

``` r
default_db()
```

## Value

A characater scalar, the name of the default database.
