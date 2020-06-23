import pandas as pd

fixes = pd.read_csv('fixes.csv', index_col='anomaly')

inFile = open('mush.csv', 'r')
outFile = open('fixedMush.csv', 'w')

fixPairs = list(zip(fixes.index, [fixes.at[anom, 'fix'] for anom in fixes.index]))


for line in inFile:
    newLine = line
    for anom,fix in fixPairs:
        if not fix == 'NO_FIX_NEEDED':
            newLine = newLine.replace(anom, fix)
    outFile.write(newLine)
    
inFile.close()
outFile.close()
