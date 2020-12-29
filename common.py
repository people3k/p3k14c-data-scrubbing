import pandas as pd
from sys import stdout

# Get df1 minus df2
def setMinus(df1, df2):
    return pd.concat([df1, df2]).drop_duplicates(keep=False)


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
STD_DEV       = 'Error'
LOC_ACCURACY  = 'LocAccuracy'
SOURCE        = 'Source'
PROVINCE      = 'Province'
D13C          = 'd13C'
FUZZ_FACTOR   = 0.5

