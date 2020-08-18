import numpy as np

n = 100
m = 1024
beta = 0.8

M = np.zeros((n, n))

file = open('../data/graph.txt', 'r') 
Lines = file.readlines()
degrees = np.zeros(n)

for line in Lines:
	edge = line.strip().split("\t")
	i = int(edge[0])-1
	j = int(edge[1])-1
	
	degrees[i] += 1
	M[j][i] += 1.0

for i in range(n):
	for j in range(n):
		if M[i][j]:
			M[i][j] /= degrees[j]


r = 1/n * np.ones((n, 1))
for i in range(40):
	r = ((1 - beta)/n)*np.ones((n, 1)) + beta*np.matmul(M, r)

r = np.reshape(r, (n,))

r_index = np.argsort(r)
r_index += 1
print("Top 5: ")
for ind in r_index[-5:]:
	print(ind, " ", r[ind-1])

print("Bottom 5: ")
for ind in r_index[:5]:
	print(ind, " ", r[ind-1])
