if (!require(dplyr)) install.packages("dplyr", repos="http://cran.us.r-project.org")
library(dplyr)

# Loas Nagappan's algorithm
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
deltas <- read.csv(input)

# Add column with all BCs excluding the ones related to: annotationDeprecatedAdded and methodAddedToPublicClass
deltas$bcs_clean = deltas$bcs - deltas$annotationDeprecatedAdded - deltas$methodAddedToPublicClass


### Clean
# Remove records whose delta cannot be computed
deltas <- subset(deltas, exception == -1) 

# Remove records whose APIs do not contain code
deltas <- subset(deltas, declarations_v1 > 0)
deltas <- subset(deltas, declarations_v2 > 0)

# Consider only Java 8 libraries (expect no changes, checked with maven-api-dataset)
deltas <- subset(deltas, java_version_v1 <= 52)
deltas <- subset(deltas, java_version_v2 <= 52)

# Remove cases where dates are used as versions (e.g. 20081010.0.1, 1.20081010.1, 1.0.20081010)
deltas <- deltas[grep("^[0-9]{0,7}[.][0-9]{0,7}([.][0-9]{0,7})?$", deltas$v1),]
deltas <- deltas[grep("^[0-9]{0,7}[.][0-9]{0,7}([.][0-9]{0,7})?$", deltas$v2),]

# Remove cases where dates are used as versions (e.g. 20081010.0.1, 1.20081010.1, 1.0.20081010)
deltas <- deltas[!grepl("^2[0-9]{3}[.][0-9]{2}([.][0-9]{2})?$", deltas$v1),]
deltas <- deltas[!grepl("^[0-9]{2}[.][0-9]{2}[.]2[0-9]{3}$", deltas$v1),]
deltas <- deltas[!grepl("^[0-9]{2}[.]2[0-9]{3}$", deltas$v1),]

deltas <- deltas[!grepl("^2[0-9]{3}[.][0-9]{2}([.][0-9]{2})?$", deltas$v2),]
deltas <- deltas[!grepl("^[0-9]{2}[.][0-9]{2}[.]2[0-9]{3}$", deltas$v2),]
deltas <- deltas[!grepl("^[0-9]{2}[.]2[0-9]{3}$", deltas$v2),]

# Remove cases where language != Java
deltas <- subset(deltas, language == "java")

# Remove cases where there are no breaking changes
deltas <- subset(deltas, bcs_clean > 0)
deltas <- subset(deltas, clients > 0)

for (i in 35:78) {
  name = colnames(deltas)[i]
  bool_name = paste('bool', name, sep='_')
  deltas[[bool_name]] <- ifelse(deltas[[name]] > 0, 1, 0)
}

# Reduce size of data frame
deltas <- deltas[c(1:24,312:314,318:361)]

# Factorizing level column
deltas$level <- factor(deltas$level)

### Output sample
#write.csv(deltas, file=output)

# Needed values
e = 0.05
p = 0.5

n0 = cochran(e, p)
sample_size = ceiling(cochran_cor(n0, nrow(deltas[-1,])))
sample_nag <- deltas[0,]

# Iterate 'till we reach 1.0
np <- next.projects(nrow(deltas), sample_nag, universe=deltas, year ~ level + clients + bcs + bool_annotationDeprecatedAdded + bool_classRemoved + bool_classNowAbstract + bool_classNowFinal + bool_classNoLongerPublic + bool_classTypeChanged + bool_classNowCheckedException + bool_classLessAccessible + bool_superclassAdded + bool_superclassRemoved + bool_interfaceAdded + bool_interfaceRemoved + bool_methodRemoved + bool_methodLessAccessible + bool_methodMoreAccessible + bool_methodReturnTypeChanged + bool_methodNowAbstract + bool_methodNowFinal + bool_methodNowStatic + bool_methodNoLongerStatic + bool_methodAddedToInterface + bool_methodNowThrowsCheckedException + bool_methodAbstractAddedToClass + bool_methodNewDefault + bool_methodAbstractNowDefault + bool_fieldNowFinal + bool_fieldNowStatic + bool_fieldNoLongerStatic + bool_fieldTypeChanged + bool_fieldRemoved + bool_fieldLessAccessible + bool_fieldMoreAccessible + bool_constructorRemoved + bool_constructorLessAccessible)
np$new.projects$score.increase <- NULL
sample <- as.data.frame.matrix(np$new.projects)

if (nrow(np$new.projects[-1,]) < sample_size) {
  deltas_nodup <- anti_join(deltas, sample)
  sample <- rbind(sample, sample_n(deltas_nodup, sample_size - nrow(np$new.projects[-1,])))
}

# Output
write.csv(sample, file=output)

