
import pandas as pd


def getWeights(indexName, weightsDir):
    weightsCsvFile = f"{weightsDir}/{indexName}.csv"
    weights = pd.read_csv(weightsCsvFile)

    #normalize weights df
    weights['Name'] = weights['Name'].apply(lambda x: f"{x[:-3]}.E" if " TI" in x else x)

    renamingDict = {}
    for col in weights.columns:
        removedString = 'Pos (Port) '
        if removedString in col:
            newColName = col[len(removedString):]
            renamingDict[col] = newColName

    weights = weights.rename(columns=renamingDict)

    return weights