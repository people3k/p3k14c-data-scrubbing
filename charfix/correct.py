from symspellpy import SymSpell, Verbosity
import numpy as np
import pandas as pd 
import os.path
import pickle
from termcolor import colored

sym_spell = SymSpell(max_dictionary_edit_distance=5)
FIXES = pd.DataFrame(columns=['fix','anomaly','contexts'])

IN_FILE = 'dates.csv'
COLS = [
      'SiteName','Country','Province','Region','Continent','Source','Reference'
    ]

def fetchFixTable():
    if os.path.isfile('fixes.csv'):
        print('Loading in existing fix table')
        return pd.read_csv('fixes.csv',index_col = 0)
    else:
        return FIXES

def saveFixTable():
    print('Saving fix table')
    FIXES.to_csv('fixes.csv')
    return


def printSuggestions(term):
    term = term.replace('(','').replace(')','')
#    suggestions = sym_spell.lookup(term, Verbosity.CLOSEST, max_edit_distance=5)
    suggestions = []
    print('Suggestions:')
    sugs = 0
    for suggestion in suggestions:
        sugs += 1
        if sugs == 10:
            break
        print(sugs, suggestion.term)
    print('')
    return suggestions


def getContext(anom):
    print('-------------------------------')
    print('{} anomaly:'.format(anom['type']))
    hiAnom = colored(anom['anom'], 'red', attrs=['bold'])
    aroundAnom = anom['contexts'][0][anom['type']].split(anom['anom'])
    print(aroundAnom[0] + hiAnom + aroundAnom[1])
    print('')
    sugs = printSuggestions(anom['anom'])
    print('Context(s):')
    df = pd.DataFrame(anom['contexts'])
    cols = list(set(['LabID','SiteName','Country',anom['type']]))
    print(df[cols].set_index('LabID').head(10))
    return sugs


# Prompt the user to fix it 
def prompt(anom):
    global FIXES
    # List context and suggestions
    suggestions = getContext(anom)
    print('[#] Suggestion | [M]anual | [F]lag for expert | [S]kip | [E]xit')
    sel = input('Select:')
    print('')
    fix = ''
    fixed = False
    # Prompt to:
    # Give any number to use the suggestion
    if sel.isdigit():
        fix = suggestions[int(sel)-1].term
        fixed = True
    # [M]anual entry
    elif sel.lower() == 'm':
        fix = input('Manual entry: ')
        fixed = True
    # [I]nclude blank
    elif sel.lower() == 'f':
        fix = 'NEEDS_EXPERT'
        fixed = True
    # [E]xit
    elif sel.lower() == 'e':
        saveFixTable()
        exit()
    # [S]kip
    elif sel.lower() == 's':
        fix = 'NO_FIX_NEEDED'
        fixed = True
    else:
        print('Invalid entry; try again')
        prompt(anom)
        return

    if fixed:
        FIXES = FIXES.append(pd.Series({
            'anomaly' : anom['anom'],
            'fix' : fix,
            'contexts' : anom['contexts']
        }), ignore_index=True)

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
        print('Reading in existing table of anomalies (anoms.pickle)...')
        f = open('anoms.pickle', 'rb')
        return pickle.load(f)
    else:
        return getAnomalies(records)

def getData():
    records = pd.read_csv(IN_FILE, low_memory=False, index_col=0)
    anoms = fetchAnomalies(records)
    return records, anoms


def main():
    global FIXES
    FIXES = fetchFixTable()
    print('Loading in correction dictionary (corpus/corpus.txt)...')
    corpus_path = 'corpus/corpus.txt'
    #sym_spell.create_dictionary(corpus_path)

    # First, scan through the dataset to detect and store each anomaly. 
    records, anoms = getData()
    anoms = [a for a in anoms if a['type'] == 'SiteName']
    print(FIXES['anomaly'])

    # Now, for every anomaly
    for anom in anoms:
        # If the anom isn't already in the fix table
        if anom['anom'] not in list(FIXES['anomaly']):
            # Prompt the user to fix it
            prompt(anom)

if __name__ == '__main__':
    main()
