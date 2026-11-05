
all: README.md

README.md: README.Rmd
	Rscript -e "library(knitr); library(methods); knit('$<', output = '$@', quiet = TRUE)"
