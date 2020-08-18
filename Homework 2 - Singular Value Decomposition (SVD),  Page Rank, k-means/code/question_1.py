import numpy as np
from scipy import linalg
import pdb

M = np.array([[1, 2], 
			  [2, 1], 
			  [3, 4], 
			  [4, 3]])

#################################################################
U, s, Vt = linalg.svd(M, full_matrices = False)
print ("U")
print(U)
print ("sigma")
print(s)
print ("V transpose")
print(Vt)

print("Verification: ")
print("Shape U: ", U.shape)
print("Shape s: ", s.shape)
print("Shape Vt: ", Vt.shape)

sigma = np.diag(s)
print("Reconstructed matrix M")
print(np.matmul(np.matmul(U, sigma), Vt))

#################################################################
Evals, Evecs = linalg.eigh(np.matmul(M.transpose(), M))
print ("Original Eigen values: ")
print(Evals)
print ("Original Eigen vectors: ")
print(Evecs)

# Rearranging the vectors based on eigenvalues
sorted_indices = np.argsort(Evals[::-1])
print(sorted_indices)
Evecs = Evecs[:,sorted_indices]
print("Reordered Evals: ")
print(Evals[sorted_indices])
print("Reordered Evecs: ")
print(Evecs)

#################################################################
diagonal = np.diag([-1, 1])
U_new = np.matmul(U, diagonal)
print(U_new)
Vt_new = Evecs.transpose()
sigma = np.diag(s)
print("Reconstructed matrix M")
print(np.matmul(np.matmul(U_new, sigma), Vt_new))
