
all: README.md

iREADME.md: README.Rmd
	Rscript -e "library(knitr); library(methods); knit('$<', output = '$@', quiet = TRUE)"
