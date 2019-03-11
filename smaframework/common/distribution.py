import numpy as np

def next(matrix):
	random = np.random.rand(*matrix.shape)
	random = np.divide(random, matrix)
	argmin = random.argmin()
	return np.unravel_index(argmin, matrix.shape)