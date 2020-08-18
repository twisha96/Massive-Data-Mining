"""
CS550 Massive Data Mining (Homework-3)
Author: Twisha Naik
Q1 Modularity
"""

import numpy as np

def modularity(A, x):
	'''
	A = Adjacency matrix
	x = Number indicating the group for each node
	'''
	n = len(A) 				# number of nodes
	k = np.sum(A, axis = 0) # degree
	m = np.sum(k)/2			# number of edges

	Q = 0
	for i in range(n):
		for j in range(n):
			Q += (A[i][j] - (k[i]*k[j])/(2*m)) * x[i] * x[j]
	Q = Q/(4*m)

	return Q

if __name__ == "__main__":

	# part - (a) original
	A = np.array([
			[0, 1, 1, 1, 0, 0, 1, 0],
			[1, 0, 1, 1, 0, 0, 0, 0],
			[1, 1, 0, 1, 0, 0, 0, 0],
			[1, 1, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 1, 1, 0],
			[0, 0, 0, 0, 1, 0, 1, 0],
			[1, 0, 0, 0, 1, 1, 0, 1],
			[0, 0, 0, 0, 0, 0, 1, 0]])

	x = [1, 1, 1, 1, -1, -1, -1, -1]
	print("Modularity of original graph: ")
	print(modularity(A, x))

	A = np.array([
			[0, 1, 1, 1, 0, 0, 0, 0],
			[1, 0, 1, 1, 0, 0, 0, 0],
			[1, 1, 0, 1, 0, 0, 0, 0],
			[1, 1, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 1, 1, 0],
			[0, 0, 0, 0, 1, 0, 1, 0],
			[0, 0, 0, 0, 1, 1, 0, 1],
			[0, 0, 0, 0, 0, 0, 1, 0]])

	x = [1, 1, 1, 1, -1, -1, -1, -1]
	print("Modularity of graph after removing (A-G): ")
	print(modularity(A, x))

	# part - b
	A = np.array([
			[0, 1, 1, 1, 0, 0, 1, 0],
			[1, 0, 1, 1, 0, 0, 0, 0],
			[1, 1, 0, 1, 0, 0, 0, 0],
			[1, 1, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 1, 1, 1],
			[0, 0, 0, 0, 1, 0, 1, 0],
			[1, 0, 0, 0, 1, 1, 0, 1],
			[0, 0, 0, 0, 1, 0, 1, 0]])

	x = [1, 1, 1, 1, -1, -1, -1, -1]
	print("Modularity of graph after adding (E-H): ")
	print(modularity(A, x))

	# part - c
	A = np.array([
			[0, 1, 1, 1, 0, 1, 1, 0],
			[1, 0, 1, 1, 0, 0, 0, 0],
			[1, 1, 0, 1, 0, 0, 0, 0],
			[1, 1, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 1, 1, 0],
			[1, 0, 0, 0, 1, 0, 1, 0],
			[1, 0, 0, 0, 1, 1, 0, 1],
			[0, 0, 0, 0, 0, 0, 1, 0]])

	x = [1, 1, 1, 1, -1, -1, -1, -1]
	print("Modularity of graph after adding (A-F): ")
	print(modularity(A, x))
