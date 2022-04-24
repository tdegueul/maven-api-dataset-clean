# Cochran's formula
cochran <- function(e, p) {
    q = 1 - p
    Z = qnorm(1 - e/2)
    return(((Z^2) * p * q) / (e^2))
}

# Cochran's correction formula
cochran_cor <- function(n0, size) {
	correction <- n0 / (1 + ((n0 - 1) / size))
	return (ifelse(correction >= size, size, correction))
}