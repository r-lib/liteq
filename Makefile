
all: inst/README.md

inst/README.md: inst/README.Rmd
	Rscript -e "library(knitr); library(methods); knit('$<', output = '$@', quiet = TRUE)"
