import sys

# First of all, check to make sure arugments are being passed properly
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage:')
        print('python visualize.py <file_name.csv>')
        exit(1)
    print('Initializing...')
 
IN_FILE_PATH = sys.argv[1]

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
from descartes import PolygonPatch

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

print('Reading in {}...'.format(IN_FILE_PATH))
records = pd.read_csv(IN_FILE_PATH,index_col=0,low_memory=False)
records = records[(records['Country'] == 'USA') | (records['Country'] == 'Canada') | (records['Source'] == 'GuedesBocinsky2018')]

print('Generating plot...')
myShape = USA_bounds[0]['shape']
BLUE = '#6699cc'
fig = plt.figure() 
ax = fig.gca() 

print('Plotting boundaries...')
for poly in tqdm(USA_bounds + CAN_bounds + GBC_bounds):
    ax.add_patch(PolygonPatch(poly['shape'], fc=BLUE, ec=BLUE, alpha=0.5, zorder=2 ))

print('Plotting record coordinates...')
plt.scatter(records['Long'],records['Lat'],c='red')

ax.axis('scaled')
plt.show()
