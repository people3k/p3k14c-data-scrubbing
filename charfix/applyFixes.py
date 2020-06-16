import pandas as pd

fixes = pd.read_csv('fixes.csv', index_col='anomaly')

inFile = open('dates.csv', 'r')
outFile = open('fixedDates.csv', 'w')

fixPairs = list(zip(fixes.index, [fixes.at[anom, 'fix'] for anom in fixes.index]))


for line in inFile:
    newLine = line
    for anom,fix in fixPairs:
        newLine = newLine.replace(anom, fix)
    outFile.write(newLine)
    
inFile.close()
outFile.close()
