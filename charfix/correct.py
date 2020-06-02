from symspellpy import SymSpell, Verbosity
import numpy as np
import pandas as pd 
import os.path
import pickle
from termcolor import colored

sym_spell = SymSpell()
corpus_path = 'specials.txt'
sym_spell.create_dictionary(corpus_path)



IN_FILE = 'mush.csv'
COLS = [
      'SiteName','Country','Province','Region','Continent','Source','Reference'
    ]

def printSuggestions(term):
    term = term.replace('(','').replace(')','')
    suggestions = sym_spell.lookup(term, Verbosity.CLOSEST)
    print('Suggestions:')
    sugs = 0
    for suggestion in suggestions:
        sugs += 1
        if sugs == 10:
            break
        print(suggestion)
    print('')


def printContext(anom):
    print('-------------------------------')
    print('{} anomaly:'.format(anom['type']))
    hiAnom = colored(anom['anom'], 'red', attrs=['bold'])
    aroundAnom = anom['contexts'][0][anom['type']].split(anom['anom'])
    print(aroundAnom[0] + hiAnom + aroundAnom[1])
    print('')
    printSuggestions(anom['anom'])
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
            stripped = str(datum).replace('-', '')\
                            .replace('\t', '')\
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
                            .replace("%", '')\
                            .replace("<", '')\
                            .replace(">", '')\
                            .replace('»', '')\
                            .replace('«', '')\
                            .replace("–", '')\
                            .replace(';', '')\
                            .replace('--', '')\
                            .replace('\n', '')\
                            .replace('(', '')\
                            .replace(')', '')\
                            .replace('_', '')
            # If it is still not alphanumeric after stripping ordinary symbols
            if not stripped.replace(' ','').isalnum():
                # Add each anomaly
                for word,stripword in list(zip(datum.split(), stripped.split())):
                    if not stripword.isalnum():
                        anoms = logAnomaly(anoms, word, col, records.loc[labid])
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
    anoms = [a for a in anoms if a['type'] != 'Reference']
    # About 1500 with refs, 3000 with
    # Now, for every anomaly
    for anom in anoms:
        # Prompt the user to fix it
        prompt(anom)

if __name__ == '__main__':
    main()
