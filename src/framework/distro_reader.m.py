
import sys
import pickle
import matplotlib.pyplot as plt
import bisect

pickledFileName = sys.argv[1]

pickledFile = open(pickledFileName, 'rb')      
distribution = pickle.load(pickledFile)
pickledFile.close() 

distribution =  [i * 100 for i in distribution] 

# plt.hist(distribution, bins='auto')
# plt.xlim(0,1250)
# plt.ylim(0,3000)
# plt.show()

#Print percentile of n
sortedDist = sorted(distribution)
n = 200
print (f"Percentile of spread at {n} cents")
print (bisect.bisect(sortedDist,n)/len(sortedDist))

print (f"Max spread: {max(sortedDist)}")
