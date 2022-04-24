if (!require(dplyr))
  install.packages("dplyr", repos="http://cran.us.r-project.org")

library(dplyr)

source("scripts/SampleSoftwareProjects-0.1.1/SampleSoftwareProjects.R")
source("scripts/cochran.R")

### Script arguments
args <- commandArgs(trailingOnly = TRUE)

# Check arguments
if (length(args) != 2) {
	stop("The script requires the input/output CSV files", call.=FALSE)
}

input = args[1]
output = args[2]

# Universe
upgrades <- read.csv(input)

sprintf("Sampling %d upgrades", nrow(upgrades))

# Sample size
e = 0.05
p = 0.5

n0 = cochran(e, p)
sample_size = ceiling(cochran_cor(n0, nrow(upgrades[-1,])))
sprintf("For (e=%.2f, p=%.2f), sample size: %d", e, p, sample_size)

sample <- sample_n(upgrades, sample_size)

# Output
write.csv(sample, file=output)
