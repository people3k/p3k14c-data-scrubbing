import pandas as pd
import numpy as np
from math import ceil
from common import getRecords, LAB_ID, LAB_CODE_FILE, LAT, LON, AGE, STD_DEV, LOC_ACCURACY, SOURCE, PROVINCE, FUZZ_FACTOR, flushMsg, setMinus

SHAPE = (0,0)


# Deal with entries bearing duplicate lab codes.
def handleDuplicates(records, graveyard=pd.DataFrame()):
    global SHAPE
    SHAPE = records.shape
    print('Handling duplicate entries')
    # Reset index so we can index by the index
    # index index index
    records = records.reset_index()
    graveyard = graveyard.reset_index()

    # Now, keep an arbitrary duplicate if both the date, precise coordinates,
    # and source datasets match
    noDups  = records.drop_duplicates(subset=[LAB_ID, AGE, LAT, LON, SOURCE])
    # Get the duplicate entries and add to the graveyard
    funeral = setMinus(records, noDups)
    funeral['removal_reason'] = 'True duplicate'
    graveyard = graveyard.append(funeral)
    # Set records to have no duplicates
    records = noDups

    # Concatenate source datasets for true duplicates. Requires some trickery.
    trueDups = records.duplicated(subset=[LAB_ID, AGE, LAT, LON], keep=False)
    concattedSourceDups = records[trueDups].groupby(LAB_ID).agg(
            lambda x: '; '.join(x) if x.name == SOURCE else x.iloc[0]
    )
    records = records.drop_duplicates(subset=[LAB_ID, AGE, LAT, LON])
    records = records.append(concattedSourceDups.reset_index())

    # Fetch the duplicates found in both UWyomingNSF2019 and CARD
    dups = records.duplicated(subset=[LAB_ID],keep=False)
    wyAndCard = (records[dups])[records[dups][SOURCE].apply(lambda x: 'CARD' in str(x) or 'UWyomingNSF2019' in str(x))]
    # Now, of the duplicated records in both UWyoming and Card, get JUST the Card ones, and remove them.
    justCard  = wyAndCard[wyAndCard[SOURCE].apply(lambda x: 'CARD' in str(x))]
    records   = setMinus(records, justCard)

    
    # It's time to mess with combining records based on coordinate data. Before
    # we do that, we should mark all of the entries with bad ages.
    # If we're looking at a date and dates don't match, we have a bad entry.
    #
    # NOTE: These are a very few number of records... Last I checked, no records
    # had bad ages like this... Keeping this code chunk to future-proof it,
    # though - Lux 
    dups = records.duplicated(subset=[LAB_ID],keep=False)
    badAges = records[dups].groupby(LAB_ID).agg( 
            lambda x: '{} (MISMATCHED)'.format(x) if x.name == AGE and mismatchingEntries(list(x),fuzzFactor=1) else x.iloc[0]
    )
    badAges = badAges[badAges[AGE].apply(lambda x: 'MISMATCHED' in str(x))]
    badAgeIDs = list(badAges.index)
    if len(badAgeIDs) > 0:
        for id in records.index:
            if records.at[id, AGE] in badAgeIDs:
                records.at[id, AGE] = '{} (MISMATCHED)'.format(records.at[id,AGE])
        
 
    # Now, for duplicate records with different locAccuracy, pick highest locaccuracy
    dups = records.duplicated(subset=[LAB_ID],keep=False)
    dupsWithSameLA = records.duplicated(subset=[LAB_ID,LOC_ACCURACY],keep=False)
    dupsWithDifferentLA = setMinus(records[dups], records[dupsWithSameLA])
    highestLA = dupsWithDifferentLA.sort_values(by=[LOC_ACCURACY], ascending=False)\
            .drop_duplicates(subset=[LAB_ID])
    records = setMinus(records, dupsWithDifferentLA)
    records.append(highestLA)

    # Sort the records by LocAccuracy so that higher LocAccuracy is chosen first
    records = records.sort_values(by=[LOC_ACCURACY], ascending=False)

    # If the lab number and the dates match, prioritize entries with lat/long info,
    # but delete entries that have existing mismatching lat/long info
    combinedDups = records.groupby(LAB_ID).agg(lambda x: combineDups(x))
    funeral = setMinus(records, combinedDups)
    funeral['removal_reason'] = 'Merged with partial duplicates into single record'
    graveyard = graveyard.append(funeral)
    records = combinedDups

    # Sort records back for algorithms that rely on LAB_ID order
    records = records.sort_values(by=[LAB_ID])

    isMismatched = lambda x: 'MISMATCHED' in str(x)
    mismatchedCoords = records[records[LAT].apply(isMismatched) | records[LON].apply(isMismatched)]
    mismatchedCoords['removal_reason'] = 'Duplicate record with mismatching coordinates'
    graveyard = graveyard.append(mismatchedCoords)

    mismatchedAge = records[records[AGE].apply(isMismatched)]
    mismatchedAge['removal_reason'] = 'Duplicate record with mismatching age'
    graveyard = graveyard.append(mismatchedAge)

    
    # Filter out bad entries
    records = records[\
          (records[LAT] != 'BADENTRY')\
        & (records[LON] != 'BADENTRY')\
        & (records[AGE] != 'BADENTRY')]

    return records, graveyard


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
    # If we're looking at d13C
    # If we're not looking at coordinates or a date, just pick the first thing
    if x.name != LAT and x.name != LAT and x.name != AGE:
        return x.iloc[0]
    # Otherwise, we have coordinates.
    # Get a list of non-nan entries
    nonNans = getNonNans(x) 
    # If the list is empty, return nan
    if len(nonNans) == 0:
        return np.nan
    # If the list contains mismatching entries, return BADENTRY
    if mismatchingEntries(nonNans):
        return '{} (MISMATCHED)'.format(x)
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
    records, _ = handleDuplicates(records)
    with open(outFileName, 'w', encoding='utf-8') as f:
        f.write(records.to_csv())
        f.close()
        print('Written to {}'.format(outFileName))
    
