if __name__ == '__main__':
    print('Initializing...')

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


# Dictionary  of US state/territory codes
us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

# Reversed version of the dictionary
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))


this_dir = dirname(abspath(__file__)) + '/'

US_shp = shapefile.Reader(this_dir + 'cb_2018_us_county_500k.shp')
all_US_shapes  = [shape(b) for b in US_shp.shapes()]
all_US_records = US_shp.records()

CA_shp = shapefile.Reader(this_dir + 'lcd_000a16a_e.shp', encoding='Latin-1')
all_CA_shapes  = [shape(b) for b in CA_shp.shapes()]
all_CA_records = CA_shp.records()

# Canadian data projection info from the reference guide (p2-160-g2016002-eng.pdf) pg 25
CAconv = Proj(
        proj='lcc', # Projection: Lambert Conformal Conic
        datum='NAD83', # Datum: North American 1983
        lat_1=49.0, # First standard parallel
        lat_2=77.0, # Second standard parallel
        lon_0=-91.866667, # Longitude of projection center
        lat_0=63.390675, # Latitude of projection center
        x_0=6200000.0, # False easting
        y_0=3000000.0 # False northing
       )

#x,y=8395429.268569998,1661955.754285
#
#lon,lat = CAconv(x,y, inverse=True)
#print(lon,lat)
#
#
#lat,lon= 43.741686, -79.387622
#
#exit()


# Read in a centroids table. We don't actually use the centroid coordinates from this
#    table anymore, but it is useful to get the state code from the county FIPS
centroids = pd.read_csv(this_dir + 'centroids.csv').set_index('FIPS')

# Fetch the names of the division and subdivions for the given point, as well as the 
#    region's centroid
def getUSInfo(lon, lat):
    US_point = Point((lon,lat))
    # Iterate through every county shape and see if the point is in one of them
    for i in range(len(all_US_shapes)):
        boundary = all_US_shapes[i]
        if US_point.within(boundary):
            fips = all_US_records[i][4]
            county = all_US_records[i][5]
            state = abbrev_us_state[centroids.at[int(fips), 'State']]
            centroid = (boundary.centroid.x, boundary.centroid.y)
            return county,state,centroid
    # If that didn't work, just pick the closest shape.
    # Computationally expensive, but, hey, we've got time.
    closest_county = min(all_US_shapes, key=US_point.distance)
    i = all_US_shapes.index(closest_county)
    fips = all_US_records[i][4]
    county = all_US_records[i][5]
    state = abbrev_us_state[centroids.at[int(fips), 'State']]
    centroid = (boundary.centroid.x, boundary.centroid.y)
    return county,state,centroid

# Same function as above, but for Canada.
def getCAInfo(lon,lat):
    # Be sure to convert to easting/northing first.
    easting,northing = CAconv(lon,lat)
    CA_point = Point((easting,northing))
    for i in range(len(all_CA_shapes)):
        boundary = all_CA_shapes[i]
        if CA_point.within(boundary):
            census_division = all_CA_records[i][1]
            province = all_CA_records[i][4]
            centroid = CAconv(boundary.centroid.x,boundary.centroid.y,inverse=True)
            return census_division,province,centroid
    # If that didn't work, just pick the closest shape.
    closest_division = min(all_CA_shapes, key=CA_point.distance)
    i = all_CA_shapes.index(closest_division)
    census_division = all_CA_records[i][1]
    province = all_CA_records[i][4]
    centroid = CAconv(boundary.centroid.x,boundary.centroid.y,inverse=True)
    return census_division,province,centroid
 

#lat,lon = 41.6666, -71.383333
#
#subdiv,div,centroid = getUSInfo(lon,lat)
#print('The point is in {}, {}. The region centroid is {}'.format(subdiv,div,centroid))
#
#
#exit()

def main():
    print('Loading in radiocarbon records...')
    records = pd.read_csv(this_dir+'radiocarbon_scrubbed.csv',index_col=0,low_memory=False)

    gb = records[records['Source'] == 'GuedesBocinsky2018']
    # Truncate to two decimal places
    for i in gb.index:
        records.at[i, 'Lat'] = np.floor(100*records.at[i, 'Lat'])/100
        records.at[i, 'Long'] = np.floor(100*records.at[i, 'Long'])/100
    

    # Fetch a slice of only NA records for reference purposes
    NArecs = pd.DataFrame(records[records['Country'].isin(['USA', 'Canada'])])

    # NaNs can screw with our libraries, so set them to (0,0) (clearly not in NA)
    NArecs['Lat'] = NArecs['Lat'].fillna(0)
    NArecs['Long'] = NArecs['Long'].fillna(0)

    print('Converting all USA/Canada coordinates to county/subprovince centroids...')

    # For every record
    for i, labID in tqdm(enumerate(NArecs.index), total=NArecs.shape[0]):
        # Get its lat and lon
        lat, lon = NArecs.at[labID, 'Lat'],NArecs.at[labID, 'Long']
        # Do nothing if the coordinates are 0 (null)
        if lat == 0 and lon == 0:
            continue
        # Fetch the region/subregion name and centroid
        if NArecs.at[labID, 'Country'] == 'USA':
    #        try:
                county,state,centroid = getUSInfo(lon,lat)
    #        except:
    #            print('')
    #            print('American exception(alism)')
    #            print(lat,lon)
    #            exit()
        else:
    #        try:
                div,prov,centroid = getCAInfo(lon,lat)
    #        except:
    #            print('')
    #            print('Canadian exception')
    #            print(lat,lon)
    #            exit()
        # Set the original record's coordinates to the centroid
        cLon, cLat = centroid
        records.at[labID, 'Lat'] = cLat
        records.at[labID, 'Long'] = cLon



    print('Exporting...')
    records.to_csv(this_dir + 'radiocarbon_scrubbed_and_fuzzed.csv')


if __name__ == '__main__':
    main()
