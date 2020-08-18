"""
CS550 Massive Data Mining (Homework-3)
Author: Twisha Naik
Q2 Spectral clustering (Finding optimal graph partition using 0 as boundary)
"""

from scipy import linalg
import numpy as np

# Laplacia matrix
L = np.array([
		[4, -1, -1, -1, 0, 0, -1, 0],
		[-1, 3, -1, -1, 0, 0, 0, 0],
		[-1, -1, 3, -1, 0, 0, 0, 0],
		[-1, -1, -1, 3, 0, 0, 0, 0],
		[0, 0, 0, 0, 2, -1, -1, 0],
		[0, 0, 0, 0, -1, 2, -1, 0],
		[-1, 0, 0, 0, -1, -1, 4, -1],
		[0, 0, 0, 0, 0, 0, -1, 1]])

index_to_node = {0:'A', 1:'B', 2:'C', 3:'D', 4:'E', 5:'F', 6:'G', 7:'H'}


# Eigen values and eigen vectors
evals, evecs = linalg.eigh(L)
print("\nEigen values: \n", evals)
print("\nEigen vectors: \n", evecs)

print("\nEigen values and corresponding eigenvectors")
for i, eigen_value in enumerate(evals):
	print ("(", i+1, ") ", eigen_value)
	print(evecs[:,i])
	print()


# Second smallest eigen value
lambda2 = evals[1]

# Eigen vector corresponding to lambda2
x = evecs[:,1]

print("\nEigen value lambda_2 = ", lambda2)
print("Eigen vector corresponding to lambda_2 = \n", x)


# Cluster-1
print("\nCluster 1:")
for n in np.argwhere(x>0):
	print(index_to_node[n[0]])

# Cluster-2
print("\n Cluster 2:")
for n in np.argwhere(x<0):
	print(index_to_node[n[0]])