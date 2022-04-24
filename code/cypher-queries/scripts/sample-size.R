source("scripts/cochran.R")

args <- commandArgs(trailingOnly = TRUE)

# Check arguments
if (length(args) != 3) {
	stop()
}

n = as.numeric(args[1])
e = as.numeric(args[2])
p = as.numeric(args[3])

n0 = cochran(e, p)
sample_size = ceiling(cochran_cor(n0, n))

write(sample_size, "sample-size.tmp")
