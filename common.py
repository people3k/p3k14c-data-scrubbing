import pandas as pd
from sys import stdout

# Fetch raw records
def getRecords(IN_FILE_PATH):
    records = pd.read_csv(IN_FILE_PATH, low_memory=False)
    return records


# Flush the buffer and print the message
def flushMsg(msg):
    stdout.flush()
    stdout.write('\033[A\r')
    print(msg)
 
## GLOBAL CONSTANTS ##
LAB_ID        = 'LabID'
LAB_CODE_FILE = 'Labs.csv'
LAT           = 'Lat'
LON           = 'Long'
AGE           = 'Age'
STD_DEV       = 'Sd'
LOC_ACCURACY  = 'LocAccuracy'
SOURCE        = 'Source'
PROVINCE      = 'Province'
FUZZ_FACTOR   = 0.5

