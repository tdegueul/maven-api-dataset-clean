if (!require(dplyr)) install.packages("dplyr", repos="http://cran.us.r-project.org")
library(dplyr)

# Load Nagappan's algorithm
source("scripts/SampleSoftwareProjects-0.1.1/SampleSoftwareProjects.R")

# Load Cochran's script
source("scripts/cochran.R")

### Script arguments
args <- commandArgs(trailingOnly = TRUE)

# Check arguments
if (length(args) != 2) {
	stop("The script requires one argument pointing to the input CSV file.", call.=FALSE)
}
input = args[1]
output = args[2]

### Universe
clients <- read.csv(input)

### Clean

# Remove cases where dates are used as versions (e.g. 20081010.0.1, 1.20081010.1, 1.0.20081010)
clients <- clients[grep("^[0-9]{0,7}[.][0-9]{0,7}([.][0-9]{0,7})?$", clients$cv),]

# Remove cases where dates are used as versions (e.g. 20081010.0.1, 1.20081010.1, 1.0.20081010)
clients <- clients[!grepl("^2[0-9]{3}[.][0-9]{2}([.][0-9]{2})?$", clients$cv),]
clients <- clients[!grepl("^[0-9]{2}[.][0-9]{2}[.]2[0-9]{3}$", clients$cv),]
clients <- clients[!grepl("^[0-9]{2}[.]2[0-9]{3}$", clients$cv),]

# Columns to factors
clients$cgroup <- factor(clients$cgroup)
clients$cartifact <- factor(clients$cartifact)
clients$cyear <- factor(clients$cyear)

### Output sample

# Needed values
e = 0.05
p = 0.5

n0 = cochran(e, p)
sample_size = ceiling(cochran_cor(n0, nrow(clients[-1,])))
sample <- clients

if (sample_size < nrow(clients[-1,])) {
  sample_nag <- clients[0,]
  np <- next.projects(nrow(clients), sample_nag, universe=clients, cartifact ~ cgroup + cyear)
  np$new.projects$score.increase <- NULL
  sample <- as.data.frame.matrix(np$new.projects)
  
  if (nrow(np$new.projects[-1,]) < sample_size) {
    print(paste("Sample size: ", sample_size))
    print(paste("NP: ", nrow(sample[-1,])))
    print(paste("Clients: ", nrow(clients[-1,])))
    print(paste("Sample size - NP: ", sample_size - nrow(sample[-1,])))
    clients_nodup <- anti_join(clients, sample)
    print(paste("Clients no dup: ", nrow(clients_nodup)))
    sample <- rbind(sample, sample_n(clients_nodup, sample_size - nrow(sample[-1,])))
    #sample <- sample[c(1:12)]
  }
}

#write.csv(sample, file=output)
write.table(sample, file=output, row.names=F, col.names=F, append=T, sep=",")

#sample <- clients[0,]

# Iterate 'till we reach 1.0
#np <- next.projects(nrow(clients), sample, universe=clients, cartifact ~ cgroup + cyear)

# Output
#write.table(np$new.projects, file=output, row.names=F, col.names=F, append=T, sep=",")

