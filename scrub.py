import sys

# First of all, check to make sure arugments are being passed properly
if __name__ == '__main__':
    if len(sys.argv) not in [3,4]:
        print('Usage:')
        print('python scrub.py <input_file_name.csv> <output_file_name.csv> [optional: graveyard_name.csv]')
        exit(1)
 
import pandas as pd
import numpy  as np
import io
from tqdm import tqdm
from math import ceil
import ftfy
from centroids.fuzz import getUSInfo, getCAInfo
from common import getRecords, LAB_ID, LAB_CODE_FILE, LAT, LON, AGE, STD_DEV, LOC_ACCURACY, SOURCE, PROVINCE, FUZZ_FACTOR, flushMsg, setMinus, embalm
from removeDuplicates import handleDuplicates

pd.options.mode.chained_assignment = None  # default='warn'

GRAVEYARD_NAME = 'graveyard.csv'

# Get a lower-case lab code given a 
# full lab number.
def codeFromLabNum(labnum):
    return ''.join(
         [i for i in str(labnum).split('-')[0]\
             if not i.isdigit()\
         ])\
             .lower()\
             .replace(' ','')\
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
             .replace('?','')\
             .replace('?','')\

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
def deleteBadLabs(records, graveyard):
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

    # Separate out the codes that will be removed for unknown lab codes
    funeral   = records[~records[LAB_ID].apply(codeFromLabNum).isin(knownCodes)]
    # Indicate a reason
    funeral['removal_reason'] = 'Unknown lab ID'
    # Graveyard's first funeral
    graveyard = embalm(funeral)

    # Clean dataset to only contain known codes
    records   = records[records[LAB_ID].apply(codeFromLabNum).isin(knownCodes)]

    # Fix known typos in lab codes
    records = fixTypos(records)

    # Check for numerals in a string
    hasNumeral = lambda s: True in [n in s for n in '0123456789']
    # Host another funeral for records without any actual numerals
    funeral = records[~records[LAB_ID].apply(hasNumeral)]
    funeral['removal_reason'] = 'Known lab ID but unknown lab code'
    graveyard = graveyard.append(embalm(funeral))
    # Remove records without any numerals
    records = records[records[LAB_ID].apply(hasNumeral)]

    # Remove records with question marks
    fil = lambda s: '?' not in s
    funeral = records[~records[LAB_ID].apply(fil)]
    funeral['removal_reason'] = 'Question mark in lab code'
    graveyard = graveyard.append(embalm(funeral))
    records   = records[records[LAB_ID].apply(fil)]

    # Scrub leading whitespace
    records[LAB_ID] = records[LAB_ID].apply(lambda s: s.lstrip())

    records = records.set_index(LAB_ID)
    return records, graveyard


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

# Add a portion of the dataset to the graveyard
def addBodies(graveyard, records, cleanerRecords, reason):
    funeral = setMinus(records, cleanerRecords)
    funeral['removal_reason'] = reason
    return graveyard.append(embalm(funeral))

# Apply miscellaneous cleaning 
def finishScrubbing(records, graveyard):
    print('Finishing miscellaneous scrubbing')
    # Remove records with null entries for age and SD
    cleanerRecs = records.dropna(subset=[AGE, STD_DEV])
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Null age and error')
    records     = cleanerRecs

    # Remove entries with integer entries for age and SD
    cleanerRecs = records[records[AGE].apply(isInteger)]
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Non-integer age')
    records     = cleanerRecs

    cleanerRecs = records[records[STD_DEV].apply(isInteger)]
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Non-integer error')
    records     = cleanerRecs


    # Explicit convert to int
    records[AGE]     = records[AGE].apply(int)
    records[STD_DEV] = records[STD_DEV].apply(int)

    # Remove dates from ~*~THE FUTURE~*~
    cleanerRecs = records[records[AGE] > 0]
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Age from the future')
    records     = cleanerRecs

    # Remove records with improbably small errors
    cleanerRecs = records[records[STD_DEV] >= 15]
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Error less than 10 years')
    records     = cleanerRecs

    # Remove records where error is greater than age
    cleanerRecs = records[records[STD_DEV] <= records[AGE]]
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Error greater than age')
    records     = cleanerRecs

    # Remove records that are too old to be meaningful
    cleanerRecs = records[records[AGE] <= 55000]
    graveyard   = addBodies(graveyard, records, cleanerRecs, 'Record older than 55,000 BP')
    records     = cleanerRecs

    # Fix country name for USA
    records['Country'] = records['Country'].apply(lambda x: 'USA' if x == 'United States' else x)


    # Properly format null entries
    records[LAT] = records[LAT].apply(justFloats)
    records[LON] = records[LON].apply(justFloats)

    # Fix a handful of weirdly formatted coordinates
    def delCoords(record):
        if record in records.index:
            records.at[record, LON ] = np.nan 
            records.at[record, LON ] = np.nan 
        return
    delCoords('M-1900')
    delCoords('M-2281')
    delCoords('GXO-676')
    delCoords('M-1483')
    delCoords('M-1602')
    delCoords('GaK-3896')

    return records, graveyard

# Fix character encodings mucked up by Excel
def fixEncoding(records):
    # A little overkill, but these are all possible columns with proper names
    # that may have special characters.
    cols = [
      'SiteName','Country','Province','Continent','Source','Reference'
    ]
    # Simple lambda to handle NaNs
    fixer = lambda x : '' if isNan(x) else ftfy.fix_encoding(x)
    # Do the thing!
    for i,col in enumerate(cols):
        flushMsg('Ensuring proper encoding for non-Latin charactes (step {}/{})'\
                .format(i+1,len(cols)))
        records[col] = records[col].apply(fixer)
    return records

# Fill in the county+state/division+province info for US and Canada dates
def fillInCountyInfo(records):
    # Fetch a slice of only NA records for reference purposes
    NArecs = pd.DataFrame(records[records['Country'].isin(['USA', 'Canada'])])

    # NaNs can screw with our libraries, so set them to (0,0) (clearly not in NA)
    NArecs['Lat'] = NArecs['Lat'].fillna(0)
    NArecs['Long'] = NArecs['Long'].fillna(0)

    print('Filling in county/division and state/province info for US/Canada dates...')

    # For every record
    for i, labID in tqdm(enumerate(NArecs.index), total=NArecs.shape[0]):
         # Get its lat and lon
        lat, lon = NArecs.at[labID, 'Lat'],NArecs.at[labID, 'Long']
        # Do nothing if the coordinates are 0 (null)
        if lat == 0 and lon == 0:
            continue
        # Fetch the region/subregion name
        if NArecs.at[labID, 'Country'] == 'USA':
           try:
                subdiv,div,centroid = getUSInfo(lon,lat)
           except:
                print('')
                print('AMERICAN EXCEPTION')
                print(lat,lon)
                exit()
        else:
            try:
               subdiv,div,centroid = getCAInfo(lon,lat)
            except:
                print('')
                print('Canadian exception')
                print(lat,lon)
                exit()
        # Set the original record's names
        records.at[labID, 'guessed_subprovince'] = subdiv


    return records

# Save the records to the output file
def save(records, outFilePath, fixEncoding=True):
    print('Exporting {}...'.format(outFilePath))
    records.to_csv(outFilePath, sep=',', encoding='utf-8')

def main():
    inFilePath, outFilePath = sys.argv[1], sys.argv[2]
    graveyardPath = 'graveyard.csv'
    if len(sys.argv) > 3:
        graveyardPath = sys.argv[3]
    records = getRecords(inFilePath)
    # Create a database of all removed records to keep track of their reason
    # for removal
    graveyard = pd.DataFrame()
    records, graveyard = deleteBadLabs(records, graveyard)
    records = convertCoordinates(records)
    records, graveyard = handleDuplicates(records, graveyard=graveyard)
    records, graveyard = finishScrubbing(records, graveyard)
    save(graveyard, graveyardPath, fixEncoding=False)
    records = fixEncoding(records)
    records = fillInCountyInfo(records)
    save(records, outFilePath)

if __name__ == '__main__':
    main()
