
import pandas as pd
import numpy as np


def getWeights(indexName, weightsDir):
    weightsCsvFile = f"{weightsDir}/{indexName}.csv"
    weights = pd.read_csv(weightsCsvFile)

    #normalize weights df
    
    #Normalize ticker names
    weights['Name'] = weights['Name'].apply(lambda x: f"{x[:-3]}.E" if " TI" in x else x)

    #Normalize date columns
    renamingDict = {}
    for col in weights.columns:
        removedString = 'Pos (Port) '
        if removedString in col:
            newColName = col[len(removedString):]
            renamingDict[col] = newColName

    weights = weights.rename(columns=renamingDict)

    #Convert num shares to percentages and replace NaN with 0
    for col in weights.columns:
        if "Name" in col:
            continue
        columnSum = weights[col].sum()
        weights[col] = weights[col].apply(lambda x: x/columnSum)
        weights[col] = weights[col].replace(np.nan, 0)

    return weights