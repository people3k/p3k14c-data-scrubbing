import reverse_geocoder as rg
import addfips; af = addfips.AddFIPS()
import pandas as pd 

print('Loading information...')

# Load in the county centroids as a dataframe
centroids = pd.read_csv('centroids.csv',index_col=2)

# Load in the records to be fuzzed
records = pd.read_csv('radiocarbon_scrubbed.csv',index_col=0)

# Fetch a slice of only NA records for reference purposes
NArecs  = pd.DataFrame(records[records['Country'].isin(['USA'])])
# NaNs can screw with our libraries, so set them to (0,0) (clearly not in NA)
NArecs['Lat'] = NArecs['Lat'].fillna(0)
NArecs['Long'] = NArecs['Long'].fillna(0)

# Construct the coordinate list suited to reverse_geocoder's tastes
NAcoords = list(zip(list(NArecs['Lat']),list(NArecs['Long'])))

# Fetch county information for every coordinate
NAdata = rg.search(NAcoords)

print('Converting NA coordinates to county centroids...')

# For every record
for i, labid in enumerate(NArecs.index):
    # Get its lat and long
    lat,long = NArecs.at[labid, 'Lat'],NArecs.at[labid, 'Long']
    # Get its state and county information
    data     = NAdata[i]
    state    = data['admin1']
    county   = data['admin2']
    # Fetch its FIPS code
    fips     = af.get_county_fips(county, state=state)

    try:
        # Try accessing the county centroid for its county.
        centroidLat  = centroids.at[int(fips), 'Lat']
        centroidLong = centroids.at[int(fips), 'Long']
    except:
        # If that fails, its because there's no coordinate info; set to 0
        centroidLat,centroidLong = 0,0

    # If coordinate info was successfully converted to a county centroid
    if centroidLat != 0 and centroidLong != 0:
        # Set the original record's coordinates to the county centroid
        records.at[labid, 'Lat'] = centroidLat
        records.at[labid, 'Lat'] = centroidLong

print('Exporting...')
records.to_csv('radiocarbon_scrubbed_and_fuzzed.csv')
