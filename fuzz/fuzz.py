### SOME USEFUL INFO ###
# GuedeesBocinsky2018 contains data from the following countries:
# South Korea, Thailand, Armenia, Pakistan, Kazakhstan, Uzbekistan, India,
# Turkmenistan, Taiwan, Mongolia, China
#
# Our geodata uses ISO3 codes to identify countries. The following are relevant:
# USA United States
# CAN Canada
# KOR South Korea
# THA Thailand
# ARM Armenia
# PAK Pakistan
# KAZ Kazakhstan
# UZB Uzbekistan
# TKM Turkmenistan
# TWN Taiwan
# MNG Mongolia
# CHN China
# Source: https://unstats.un.org/unsd/tradekb/Knowledgebase/Country-Code
# Retrieved 5 April 2021

import sys

# First of all, check to make sure arugments are being passed properly
if __name__ == '__main__':
    if len(sys.argv) not in [3,4]:
        print('Usage:')
        print('python fuzz.py <input_file_name.csv> <output_file_name.csv>')
        exit(1)
    print('Initializing...')
 

IN_FILE_PATH, OUT_FILE_PATH = sys.argv[1], sys.argv[2]

import shapefile
import numpy as np
from shapely.geometry import Point
from shapely.geometry import shape
from shapely.ops import nearest_points
import matplotlib.pyplot as plt
import pandas as pd
from pyproj import Proj
from tqdm import tqdm
from os.path import dirname, abspath


this_dir = dirname(abspath(__file__)) + '/'

print('Reading in shapefile...')
global_shp = shapefile.Reader(this_dir + 'geoBoundariesCGAZ_ADM2.shp')
global_records = global_shp.records()

print('Processing data from relevant countries...')
# Country code is at position 3
COUNTRY_CODE = 3

# Bounds for USA, Canada, and GuesdesBocinsky2018 (GBC)
USA_bounds = []
CAN_bounds = [] 
GBC_bounds = []

# Codes for GuedesBocsinky2018. See top of file. 
GBC_countries = ['KOR','THA','ARM','PAK','KAZ','UZB','TKM','TWN','MNG','CHN']

# Quick function to store our data in a pretty way
makePair = lambda s,i : {'shape': shape(s), 'record': global_records[i]}
   
# For every shape
for i,s in enumerate(global_shp.shapes()):
   # Get our country
    country = global_records[i][COUNTRY_CODE]
    if country == 'USA':
        USA_bounds.append(makePair(s,i))
    elif country == 'CAN':
        CAN_bounds.append(makePair(s,i))
    elif country in GBC_countries:
        GBC_bounds.append(makePair(s,i))

testlat,testlon = 40.62411504008629, -111.85376945306159

# Get the ADMIN2 centroid given a point and the bound set to use
# Returns lon,lat
def toCentroid(lat,lon,bound_set=USA_bounds):
    point = Point((lon,lat))
    # Iterate through every shape in the set to see if the point is in one
    for i in range(len(bound_set)):
        boundary = bound_set[i]['shape']
        if point.within(boundary):
            return boundary.centroid.x,boundary.centroid.y
    return 0,0

print(toCentroid(testlat,testlon))

#print('Reading in radiocarbon records...')
#records = pd.read_csv(IN_FILE_PATH,index_col=0)

