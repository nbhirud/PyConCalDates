
import numpy as np
 
# using loadtxt()
arr = np.loadtxt("sample_data.csv",
                 delimiter=",", dtype=str)
print(arr)

