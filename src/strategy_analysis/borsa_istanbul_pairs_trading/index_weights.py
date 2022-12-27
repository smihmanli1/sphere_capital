
import pandas as pd


def getWeights(indexName, weightsDir):
    weightsCsvFile = f"{weightsDir}/{indexName}.csv"
    weights = pd.read_csv(weightsCsvFile)
    print (weights)
    return {indexName : 1}