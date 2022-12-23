import glob
import json
import sys
from collections import defaultdict
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as ps

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


# for ticker1 in allCorrelations:
#     for ticker2 in allCorrelations[ticker1]:
#         fig = px.histogram(allCorrelations[ticker1][ticker2])
#         fig.show()


# fig = px.histogram(allCorrelations['BIST30']['ZPX30.F'], title='BIST30_ZPX30.F', nbins=20)
# fig.show()


hist1 = go.Histogram(x=allCorrelations['BIST30']['ZPX30.F'], nbinsx=20)
hist2 = go.Histogram(x=allCorrelations['BIST30']['DJIST.F'], nbinsx=20)

# Create a figure with two subplots
fig = ps.make_subplots(rows=1, cols=2)

# Add the first histogram to the first subplot
fig.add_trace(hist1, row=1, col=1)

# Add the second histogram to the second subplot
fig.add_trace(hist2, row=1, col=2)

# Display the figure
fig.show()






