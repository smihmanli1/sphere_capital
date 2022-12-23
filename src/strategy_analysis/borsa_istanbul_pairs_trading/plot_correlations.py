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
newHists = []
addedCorrelations = set()
for ticker1 in allCorrelations:
    for ticker2 in allCorrelations[ticker1]:
        #Do not add the histogram for the same correlation twice
        if (ticker2,ticker1) in addedCorrelations:
            continue
        allTexts = f"{ticker1}_{ticker2}"
        newHist = go.Histogram(x=allCorrelations[ticker1][ticker2],nbinsx=20, name=allTexts, text=allTexts, textposition='auto')
        newHists.append(newHist)
        addedCorrelations.add((ticker1,ticker2))
        
        
numColumns = 5
fig = ps.make_subplots(rows=math.ceil(len(newHists)/numColumns), cols=numColumns)

count = 0
for newHist in newHists:
    fig.add_trace(newHist, row=int(count/numColumns)+1, col=(count % numColumns)+1)
    count += 1


fig.update_layout(title='title',
    autosize=True,
    height=4000,
)

fig.show()






