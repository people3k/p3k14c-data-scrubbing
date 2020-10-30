import pandas as pd
import numpy as np
from math import ceil
from common import getRecords, LAB_ID, LAB_CODE_FILE, LAT, LON, AGE, STD_DEV, LOC_ACCURACY, SOURCE, PROVINCE, FUZZ_FACTOR, flushMsg

SHAPE = (0,0)

# Deal with entries bearing duplicate lab codes.
def handleDuplicates(records):
    global SHAPE
    SHAPE = records.shape
    print('Handling duplicate entries')
    # Reset index so we can index by the index
    # index index index
    records = records.reset_index()

    # Now, keep an arbitrary duplicate if both the date, precise coordinates,
    # and source datasets match
    records = records.drop_duplicates(subset=[LAB_ID, AGE, LAT, LON, SOURCE])

    # Sort the records by LocAccuracy so that higher LocAccuracy is chosen first
    records = records.sort_values(by=[LOC_ACCURACY], ascending=False)

    # If the lab number and the dates match, prioritize entries with lat/long info,
    # but delete entries that have existing mismatching lat/long info
    records = records.groupby(LAB_ID).agg(lambda x: combineDups(x))

    # Sort records back for algorithms that rely on LAB_ID order
    records = records.sort_values(by=[LAB_ID])

    # Filter out bad entries
    records = records[\
          (records[LAT] != 'BADENTRY')\
        & (records[LON] != 'BADENTRY')\
        & (records[AGE] != 'BADENTRY')]

    return records


# Input:  a list of numbers
# Output: A boolean indicating:
#           False - Every number in the list is within 0.5 of each other
#           True  - The list contains numbers with a larger difference than 0.5
def mismatchingEntries(nums, fuzzFactor=FUZZ_FACTOR):
    # Calculate all magnitude differenes
    for i in nums:
        for j in nums:
            # If the magnitude of any difference is larger than the allowed
            # margins, the data is mismatched
            if np.abs(i-j) > fuzzFactor:
                return True
    # No mismatched data => matching data
    return False


# Combiner function to handle duplicates
def combineDups(x):
    printProgress()
    # If there's nothing to combine, just pick the first thing
    if len(x) == 1:
        return x.iloc[0]
    # If we're looking at source datasets
    if x.name == SOURCE:
       # Concatenate all sources together
       return ', '.join(x)
    # If we're not looking at coordinates or a date, just pick the first thing
    if x.name != LAT and x.name != LAT and x.name != AGE:
        return x.iloc[0]
    # If we're looking at a date and dates don't match, we have a bad entry.
    if x.name == AGE and mismatchingEntries(list(x),fuzzFactor=1):
        return 'BADENTRY'
    # Otherwise, we have coordinates.
    # Get a list of non-nan entries
    nonNans = getNonNans(x) 
    # If the list is empty, return nan
    if len(nonNans) == 0:
        return np.nan
    # If the list contains mismatching entries, return BADENTRY
    if mismatchingEntries(nonNans):
        return 'BADENTRY'
    # Otherwise, get the first coordinate in the list of nonNans
    firstCoord = nonNans[0]
    # If that first coordinate is a 0, return nan
    if firstCoord == 0:
        return np.nan
    # Otherwise, return the first coordinate
    return firstCoord

# A function for printing the progress of duplicate handling, since it 
# is the longest process of the scrubber
dupsProcessed = 0
def printProgress():
    global dupsProcessed
    dupsProcessed += 1
    # Every 100 fields
    if dupsProcessed % 1000 == 0:
        # Total records processed is the number of entries divided by columns
        recordsProcessed = dupsProcessed/SHAPE[1]
        percentComplete  = ceil(recordsProcessed/SHAPE[0]*(400/3))
        flushMsg('Handling duplicate entries ({}% complete)'\
                .format(percentComplete))

# Input:   x - pandas series, potentially containing different types.
# Output:  A list of those entries that are both valid coordinates
#          and not NaN 
def getNonNans(x):
    nonNans = []
    for item in x:
        try:
            num = float(item)
            if not np.isnan(num):
                nonNans.append(num)
        except:
            continue

    return nonNans


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print('Usage: python removeDuplicates.py input_file.csv out_file.csv')
        exit(1)
    inFile = sys.argv[1]
    outFileName = sys.argv[2]
    records = getRecords(inFile)
    records = handleDuplicates(records)
    with open(outFileName, 'w', encoding='utf-8') as f:
        f.write(records.to_csv())
        f.close()
        print('Written to {}'.format(outFileName))
    
