import glob
import json
import sys
from collections import defaultdict
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as ps
import math

def ingestCorrelations(correlationsDict, result):
    for mainCorrelation in correlationsDict:
        for toCorrelation in correlationsDict[mainCorrelation]:
            result[mainCorrelation][toCorrelation].append(correlationsDict[mainCorrelation][toCorrelation])
    return result

def consolidateCorrelations(correlationsDir):
    wildcard = f"{correlationsDir}/*_correlations.json"
    allCorrelationFiles = glob.glob(wildcard)
    allCorrelations = defaultdict(lambda: defaultdict(list))

    for correlationFile in allCorrelationFiles:
        with open(correlationFile) as f:
            correlations = json.load(f)
            allCorrelations = ingestCorrelations(correlations, allCorrelations)

    return allCorrelations


allCorrelations = consolidateCorrelations(sys.argv[1])


# Create a figure with two subplots
numColumns = 2
fig = ps.make_subplots(rows=math.ceil(math.pow(len(allCorrelations),2)/numColumns), cols=numColumns)
count = 0
for ticker1 in allCorrelations:
    if count == 2:
        break
    for ticker2 in allCorrelations[ticker1]:
        # print (allCorrelations[ticker1][ticker2]) 
        # print (count)
        newHist = go.Histogram(x=allCorrelations[ticker1][ticker2])
        print (allCorrelations[ticker1][ticker2])
        print (f"Row: {int(count/numColumns)+1}")
        print (f"Col: {(count % numColumns)+1}")
        fig.add_trace(newHist, row=int(count/numColumns)+1, col=(count % numColumns)+1)
        count += 1
        if count == 2:
            break
        
fig.show()






