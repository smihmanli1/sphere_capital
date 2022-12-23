import glob
import json
import sys
from collections import defaultdict


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




