from symspellpy import SymSpell, Verbosity
import numpy as np
import pandas as pd 
import os.path
import pickle

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
 
def printContext(anom):
    print('-------------------------------')
    print('{} anomaly:'.format(anom['type']))
    print(anom['anom'])
    print('')
    print('Context(s):')
    df = pd.DataFrame(anom['contexts'])
    cols = list(set(['LabID','SiteName','Country',anom['type']]))
    print(df[cols].set_index('LabID').head(10))


# Prompt the user to fix it 
def prompt(anom):
    # List context and suggestions
    printContext(anom)
    lol = input('Select:')
    print('')
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

def addNewAnomaly(anoms, index, datum, col, context):
    newAnom = {
            'anom': datum,
            'type' : col,
            'contexts' : [context]
            }
    return (anoms[:index] + [newAnom] + anoms[index:])

def logAnomaly(anoms, datum, col, context):
    index, found = contains(anoms, datum)
    if found:
        return addContext(anoms, index, context)
    else:
        return addNewAnomaly(anoms, index, datum, col, context)

def getAnomalies(records):
    print('Detecting anomalies...')
    anoms = []
    # For every record
    for labid in records.index:
        # For every potentially anomalous feature type
        for col in COLS:
            datum = records.at[labid, col]
            stripped = str(datum).replace(' ', '')\
                            .replace('\t', '')\
                            .replace('-', '')\
                            .replace('/', '')\
                            .replace(':', '')\
                            .replace('.', '')\
                            .replace(',', '')\
                            .replace('"', '')\
                            .replace("'", '')\
                            .replace("=", '')\
                            .replace("&", '')\
                            .replace("-", '')\
                            .replace("*", '')\
                            .replace("°", '')\
                            .replace("\\", '')\
                            .replace("[", '')\
                            .replace("]", '')\
                            .replace("#", '')\
                            .replace("+", '')\
                            .replace("–", '')\
                            .replace(';', '')\
                            .replace('--', '')\
                            .replace('\n', '')\
                            .replace('(', '')\
                            .replace(')', '')\
                            .replace('_', '')
            # If it is still not alphanumeric after stripping ordinary symbols
            if not stripped.isalnum():
                # Add it to the anomalies
                anoms = logAnomaly(anoms, datum, col, records.loc[labid])
    with open('anoms.pickle', 'wb') as f:
        pickle.dump(anoms, f)
    return anoms

def fetchAnomalies(records):
    if os.path.isfile('anoms.pickle'):
        f = open('anoms.pickle', 'rb')
        return pickle.load(f)
    else:
        return getAnomalies(records)

def getData():
    records = pd.read_csv(IN_FILE, low_memory=False, index_col=0)
    anoms = fetchAnomalies(records)
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
