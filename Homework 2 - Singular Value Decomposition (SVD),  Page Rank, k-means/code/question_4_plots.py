import matplotlib.pyplot as plt
import numpy as np

# Reading the cost of c1
file1 = open('../data/c1_centroid_results/cost.txt', 'r') 
lines_c1 = file1.readlines()

# Reading the cost of c2
file2 = open('../data/c2_centroid_results/cost.txt', 'r') 
lines_c2 = file2.readlines()

c1_cost = []
c2_cost = []

for costs in zip(lines_c1, lines_c2):
	c1_cost.append(float(costs[0]))
	c2_cost.append(float(costs[1]))

# multiple line plot
plt.plot(c1_cost, marker='o', markerfacecolor='blue', markersize=5, color='skyblue', linewidth=2, label="c1.txt")
plt.plot(c2_cost, marker='*', markerfacecolor='green', markersize=8, color='lightgreen', linewidth=2, label="c2.txt")
plt.xticks(ticks=np.arange(0, 20, step=1), labels=np.arange(1, 21, step=1))
plt.xlabel("Iteration number")
plt.ylabel("Cost")
plt.title("Cost w.r.t. iteration number")
plt.legend()
plt.show()