import pandas as pd
import numpy  as np
import io
from tqdm import tqdm
from sys import stdout
from math import ceil
import ftfy


## GLOBAL CONSTANTS ##
IN_FILE_PATH  = 'radiocarbon_raw.csv'
FILE_NAME     = 'radiocarbon_raw.csv'
OUT_FILE      = 'radiocarbon_scrubbed.csv'
LAB_ID        = 'LabID'
LAB_CODE_FILE = 'Labs.csv'
LAT           = 'Lat'
LON           = 'Long'
AGE           = 'CRA'
STD_DEV       = 'Sd'
LOC_ACCURACY  = 'LocAccuracy'
SOURCE        = 'Source'
FUZZ_FACTOR   = 0.5
PARENT_CHILD_FILE = 'Parent_Child_Table.csv'

pd.options.mode.chained_assignment = None  # default='warn'
SHAPE         = (0,0)

# Get a lower-case lab code given a 
# full lab number.
def codeFromLabNum(labnum):
    return ''.join(
         [i for i in str(labnum).split('-')[0]\
             if not i.isdigit()\
         ])\
             .lower()\
             .replace(' ','')\
             .replace('*','')\
             .replace('‐','')\
             .replace('_','')\
             .replace(',','')\
             .replace('(','')\
             .replace(')','')\
             .replace('#','')\
             .replace('/','')\
             .replace('’','')\
             .replace('&','')\

# Fetch the raw records
def getRecords():
    global SHAPE
    print('Fetching raw records')
    records = pd.read_csv(IN_FILE_PATH, low_memory=False)
    SHAPE = records.shape
    return records

# Fixer function
def replaceTypo(labNum, actualCode):
    typoCode = codeFromLabNum(labNum)
    return labNum.lower().replace(typoCode,actualCode)

# Fix known typos in lab codes
def fixTypos(records):
    # Fetch typo'd lab codes
    labCodes  = pd.read_csv('Labs.csv')
    labCodes  = labCodes[labCodes['PARENT_CODE'].notnull()]
    typoCodes = list(labCodes['CODE'])
    labCodes  = labCodes.set_index('CODE')
    # Apply the fixer
    records[LAB_ID] = records[LAB_ID].apply(lambda labNum:
            replaceTypo(labNum, labCodes.at[codeFromLabNum(labNum),'PARENT_CODE']) if labNum in typoCodes else labNum
    )
    return records


# Remove records with unknown lab codes,
# and generate a list of unknown lab identifiers.
def deleteBadLabs(records):
    print('Removing records with unknown lab codes')
    # Read in list of known, unique, lower-case lab codes
    knownCodes = pd.read_csv('Labs.csv')['CODE'].apply(lambda s: s.lower()).unique()
    # Fetch all unique lab codes in the input file
    allCodes   = records[LAB_ID].apply(codeFromLabNum).unique()
    # Get the list of codes that aren't in knownCodes
    unknownCodes = [code for code in allCodes if not code in knownCodes]

    # Create a csv of unknown codes
    pd.DataFrame(unknownCodes, columns=['Code'])\
        .set_index('Code')\
        .to_csv('unknown_codes.csv')

    print('Saved unknown codes to unknown_codes.csv')

    # Clean dataset to only contain known codes
    records = records[records[LAB_ID].apply(codeFromLabNum).isin(knownCodes)]

    # Fix known typos in lab codes
    records = fixTypos(records)

    records = records.set_index(LAB_ID)
    return records


# Some hot jank 
def degMinSecToDec(coord, axis):
    coord = coord.replace(',','')
    comps = coord.replace(' ','').split('*')
    degs  = int(comps[0])
    comps = comps[1].split('\'')
    mins  = comps[0]
    if mins[-1] in ['N','E','S','W']:
        mins = mins[:-1]
    mins  = int(mins)
    secs  = 0
    if '"' in comps:
        comps = comps[1].split('"')
        secs  = int(comps[0])
    factor = -1 if axis == 'long' else 1
    return factor*(degs + (mins/60.0) + (secs/3600.0))

# Degree/minute/second coords all have
# a '*' to represent the degree symbol
def isDegMinSec(coord):
    return '*' in coord

# Check if the coordinate is in Northing/Easting format
def isSolheim(coord):
    return (coord[-1] == 'N' or coord[-1] == 'E')

# Convert Northing/Easting to decimal
def solheimToDec(coord):
    comps = coord.split(' ')
    return degMinSecToDec(comps[0] + '*' + comps[1] + '\'', 'solheim')


# Convert a single coordinate along either lat or lon
def convertCoord(coord, axis):
    # If the coordinate is already in proper format,
    # no need to convert.
    if isinstance(coord, float):
        return coord
    # Convert degree/minute/second format
    if isDegMinSec(coord):
        return degMinSecToDec(coord,axis)
    # Solheim formats coords as Northing/Easting
    elif isSolheim(coord):
        return solheimToDec(coord)
    else:
        return coord

# Convert a latitude coordinate
def convertLat(lat):
    return convertCoord(lat, 'lat')
 
# Convert a longitude coordinate
def convertLon(lon):
    return convertCoord(lon, 'lat')


# Convert coordinates from Robinson/Bird and Solheim
# which are formatted differently to standard decimal format.
def convertCoordinates(records):
    print('Homogenizing coordinate formats')
    records[LAT] = records[LAT].apply(convertLat)
    records[LON] = records[LON].apply(convertLon)
    return records

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

# Given a list of at least two sources, choose the older one from the tree.
def oldestSource(sources, familyTree):
    # Quite simply, if the source has no parents, choose that one
    for source in sources:
        if isNan(familyTree.at[source, 'ParentDatasets']):
            return source
    print('Error on line 197: Unable to use simple parentage rule to determine \
            most senior dataset among the following sources: {}. Please contact\
            a developer to implement handling for more complex cases.'\
                .format(sources))
    exit(1)

# Flush the buffer and print the message
def flushMsg(msg):
    stdout.flush()
    stdout.write('\033[A\r')
    print(msg)
 
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


# Combiner function to handle duplicates
def combineDups(x, familyTree):
    printProgress()
    # If there's nothing to combine, just pick the first thing
    if len(x) == 1:
        return x.iloc[0]
    # If we're looking at source datasets
    if x.name == SOURCE:
        # Prioritize the oldest ones in the family tree
        return oldestSource(x, familyTree)
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

# A magical function that is infinitely and arcanely more powerful than np.isnan
def isNan(thing):
    return str(thing) == 'nan'

def recursiveYield(thing):
    if isinstance(thing, list):
        for sub in thing:
            yield from flatten(sub)
    else:
        yield thing

# Flatten an n-dimensional list
def flatten(ndlist):
    return list(recursiveYield(ndlist))


def getPrevGen(table, parentList):
    # No parents
    if isNan(parentList) or parentList == []:
        return []
   
    validParents = [parent for parent in parentList if parent in list(table.index)]
    parentParents = [table.at[parent, 'ParentDatasets'] for parent in validParents]
    parentParents = [p for p in flatten(parentParents) if not isNan(p)]
    return parentParents + flatten(getPrevGen(table, parentParents))
    

# Include all parent information for child datasets so that the oldest ancestor
# can be prioritized. 
def includeAllParents(table):
    # Oldest ancestors are listed last
    table['ParentDatasets'] = table['ParentDatasets'].apply(
            lambda parentList :
            parentList if isNan(parentList) else parentList + getPrevGen(table,parentList)
        )
    return table

# Get the parent child tree and include recursive info for all parents
def getParentChildTree():
    table = pd.read_csv(PARENT_CHILD_FILE, index_col=0)
    # Convert strings to list
    table['ParentDatasets'] = table['ParentDatasets'].apply(lambda s:
            np.nan if isNan(s) else list(map(lambda i: i.strip(), s.split(',')))
            )
    table = includeAllParents(table)
    return table
 
# Deal with entries bearing duplicate lab codes.
def handleDuplicates(records):
    print('Handling duplicate entries')
    # Reset index so we can index by the index
    # index index index
    records = records.reset_index()

    # Now, keep an arbitrary duplicate if both the date, precise coordinates,
    # and source datasets match
    records = records.drop_duplicates(subset=[LAB_ID, AGE, LAT, LON, SOURCE])

    # Sort the records by LocAccuracy so that higher LocAccuracy is chosen first
    records = records.sort_values(by=[LOC_ACCURACY], ascending=False)

    # Fetch the parent-child tree
    familyTree = getParentChildTree()

    # If the lab number and the dates match, prioritize entries with lat/long info,
    # but delete entries that have existing mismatching lat/long info
    records = records.groupby(LAB_ID).agg(lambda x: combineDups(x, familyTree))

    # Sort records back for algorithms that rely on LAB_ID order
    records = records.sort_values(by=[LAB_ID])

    # Filter out bad entries
    records = records[\
          (records[LAT] != 'BADENTRY')\
        & (records[LON] != 'BADENTRY')\
        & (records[AGE] != 'BADENTRY')]

    return records

# Janky function to see if a given object is a round number
# (Accepts strings and other weird things too)
def isInteger(x):
    try:
        return (float(x) == int(x))
    except:
        return False

# Convert x to a float NO MATTER WHAT, YOU HEAR?
# Converts strings etc. to NaN
def justFloats(x):
    return pd.to_numeric(x, errors='coerce')

# Apply miscellaneous cleaning 
def finishScrubbing(records):
    print('Finishing miscellaneous scrubbing')
    # Remove records with null entries for age and SD
    records = records.dropna(subset=[AGE, STD_DEV])

    # Remove entries with non-integer entries for age and SD
    records = records[records[AGE].apply(isInteger)]
    records = records[records[STD_DEV].apply(isInteger)]

    # Explicit convert to int
    records[AGE]     = records[AGE].apply(int)
    records[STD_DEV] = records[STD_DEV].apply(int)

    # Remove dates from ~*~THE FUTURE~*~
    records = records[records[AGE] > 0]

    # Remove StdDevs that are too large for our tastes
    records = records[(records[STD_DEV] >= 10) & (records[STD_DEV] <= 300)]
    records = records[records[STD_DEV] <= records[AGE]]

    # Remove records that are too old to be meaningful
    records = records[records[AGE] <= 43500]

    # Properly format null entries
    records[LAT] = records[LAT].apply(justFloats)
    records[LON] = records[LON].apply(justFloats)

    # Fix a handful of weirdly formatted coordinates
    records.at['M-2212/M-2213', LAT ] = np.nan 
    records.at['M-1900',        LAT ] = np.nan 
    records.at['M-2281',        LAT ] = np.nan 
    records.at['GXO-676',       LAT ] = np.nan 
    records.at['M-1483',        LAT ] = np.nan 
    records.at['M-1602',        LAT ] = np.nan 
    records.at['GaK-3896',      LAT ] = np.nan 
    records.at['M-2212/M-2213', LON ] = np.nan 
    records.at['M-1900',        LON ] = np.nan 
    records.at['M-2281',        LON ] = np.nan 
    records.at['GXO-676',       LON ] = np.nan 
    records.at['M-1483',        LON ] = np.nan 
    records.at['M-1602',        LON ] = np.nan 
    records.at['GaK-3896',      LON ] = np.nan 

    return records



# Fix character encodings mucked up by Excel
def fixEncoding(records):
    # A little overkill, but these are all possible columns with proper names
    # that may have special characters.
    cols = [
      'SiteName','Country','Province','Region','Continent','Source','Reference'
    ]
    # Simple lambda to handle NaNs
    fixer = lambda x : '' if isNan(x) else ftfy.fix_encoding(x)
    # Do the thing!
    for i,col in enumerate(cols):
        flushMsg('Ensuring proper encoding for non-Latin charactes (step {}/{})'\
                .format(i+1,len(cols)))
        records[col] = records[col].apply(fixer)
    return records

# Save the records to the output file
def save(records):
    outFile = open(OUT_FILE,'w')
    outFile.write(ftfy.fix_text(records.to_csv()))
    outFile.close()

def main():
    records = getRecords()
    records = deleteBadLabs(records)
    records = convertCoordinates(records)
    records = handleDuplicates(records)
    records = finishScrubbing(records)
    records = fixEncoding(records)
    save(records)

if __name__ == '__main__':
    main()
