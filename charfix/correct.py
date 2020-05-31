from symspellpy import SymSpell, Verbosity
import numpy as np
import pandas as pd 
"""
sym_spell = SymSpell()
corpus_path = 'specials.txt'
sym_spell.create_dictionary(corpus_path)

input_term = 'bai'

suggestions = sym_spell.lookup(input_term, Verbosity.CLOSEST)

for suggestion in suggestions:
    print(suggestion)
"""

IN_FILE = 'mush.csv'
COLS = [
      'SiteName','Country','Province','Region','Continent','Source','Reference'
    ]
 

# Prompt the user to fix it 
def prompt(anom):
    print(anom)
    # List context and suggestions
    # Prompt to:
        # [#] Choose suggestion
            # Prompt to fix [o]nce
                # Change the data structure in-place
            # Prompt to fix [a]ll
                # Add to fix-all dictionary
        # [F]lag for expert
            # Change the data-structure in-place
        # [L]eave alone
            # Skip
        # [M]anual entry
            # Take manual entry
            # Change structure in-place
    return

# Simple binary search
def binarySearch(anoms, datum, l, r):
    mid = int((l+r)/2)
    if l > r:
        return mid,False
    elif datum > anoms[mid]['anom']:
        return binarySearch(anoms, datum, mid+1, r)
    elif datum < anoms[mid]['anom']:
        return binarySearch(anoms, datum, l, mid-1)
    else:
        return mid,True

def contains(anoms, datum):
    return binarySearch(anoms, datum, 0, len(anoms) - 1)

def addContext(anoms, index, context):
    anoms[index]['contexts'].append(context)
    return anoms

def addNewAnomaly(anoms, index, datum, context):
    newAnom = {
            'anom': datum,
            'contexts' : [context]
            }
    return (anoms[:index] + [newAnom] + anoms[index:])

def logAnomaly(anoms, datum, context):
    index, found = contains(anoms, datum)
    if found:
        return addContext(anoms, index, context)
    else:
        return addNewAnomaly(anoms, index, datum, context)

def getAnomalies(records):
    anoms = []
    # For every record
    for labid in records.index:
        # For every potentially anomalous feature type
        for col in COLS:
            datum = records.at[labid, col]
            stripped = str(datum).replace(' ', '')\
                            .replace('-', '')\
                            .replace('/', '')\
                            .replace(':', '')\
                            .replace('.', '')\
                            .replace(',', '')\
                            .replace('"', '')\
                            .replace("'", '')\
                            .replace("=", '')\
                            .replace("&", '')\
                            .replace("°", '')\
                            .replace("\\", '')\
                            .replace("[", '')\
                            .replace("]", '')\
                            .replace("#", '')\
                            .replace("+", '')\
                            .replace("–", '')\
                            .replace(';', '')\
                            .replace('(', '')\
                            .replace(')', '')\
                            .replace('_', '')
            # If it is still not alphanumeric after stripping ordinary symbols
            if not stripped.isalnum():
                # Add it to the anomalies
                anoms = logAnomaly(anoms, datum, records.loc[labid])
    return anoms


def getData():
    records = pd.read_csv(IN_FILE, low_memory=False, index_col=0)
    anoms = getAnomalies(records)
    return records, anoms


def main():
    # First, scan through the dataset to detect and store each anomaly. 
    records, anoms = getData()
    # Now, for every anomaly
    for anom in anoms:
        # Prompt the user to fix it
        prompt(anom)

if __name__ == '__main__':
    main()
