#import required packages
import pandas as pd
import geopandas as gpd
import requests
import json
import shapely
import fiona
from pyproj import Proj, transform, Transformer
from sqlalchemy import create_engine
import psycopg2
import geoalchemy2


def postgis_push(spatial_df):

    '''
    A function to set the correct srid and then push the data to a local postgis server
    '''
    #defines a crs
    projected_df = spatial_df.set_crs(epsg=4326)

    #pushes data to db
    db_connection_url = "postgres://postgres:postgres@localhost:5432/GMaps"
    engine = create_engine(db_connection_url)
    projected_df.to_postgis(name="projected_test", con=engine)

def csv_to_point(non_spatial_data):

    '''
    Use the merged non-spatial DF to create a geopandas DF with point shapefiles from the lat/lng data
    '''

    #build the geodataframe and export it as a pointfile
    del non_spatial_data['Tags']
    spatial_df = gpd.GeoDataFrame(non_spatial_data, geometry=gpd.points_from_xy(non_spatial_data.Longitude, non_spatial_data.Latitude))
    spatial_df.to_csv("point_data.csv")
    print(spatial_df)
    spatial_df.to_file("point_data.shp")

    #create a projection file that corresponds to where data was taken from
    prj = open("point_data.prj", "w")
    epsg = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
    prj.write(epsg)
    prj.close()

    return(spatial_df)

def find_locations(search_url, api_key):

    '''
    Accesses google's api to find the location of nearby establishments
    Requires a https google api request
    '''

    #list of lists for all data
    final_data = []

    #while loop to request and parse through the requested JSON files
    while True:
        respon = requests.get(search_url)
        jj = json.loads(respon.text)
        results = jj['results']
        #parse out all the required info
        for result in results:
            name = result['name']
            place_id = result ['place_id']
            lat = result['geometry']['location']['lat']
            longi = result['geometry']['location']['lng']
            rating = result['rating']
            types = result['types']
            data = [name, place_id, lat, longi, rating, types]
            final_data.append(data)
        
        #if there is a next page the loop restarts with an updated url
        #if there is no next page the program writes to a csv and saves a df
        if 'next_page_token' not in jj:
            labels = ['Place Name','ID_Field', 'Latitude', 'Longitude', 'Rating', 'Tags']
            location_df = pd.DataFrame.from_records(final_data, columns=labels)
            location_df.to_csv('location.csv')
            break
        else:
            next_page_token = jj['next_page_token']
            search_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key='+str(api_key)+'&pagetoken='+str(next_page_token)

    return(final_data, location_df)

def find_details(final_data, api_key):

    '''
    Uses the ID_Field of establishmnets to find thier phone numbers, ratings, email and business names 
    '''

    final_detailed_data =[]

    #Uses the unique ID of each location to use another API request to pull phone and website information for each business. 
    for places in final_data:
        id_field = places[1]
        req_url = 'https://maps.googleapis.com/maps/api/place/details/json?place_id='+str(id_field)+'&fields=name,formatted_phone_number,website&key='+str(api_key)
        respon = requests.get(req_url)
        jj = json.loads(respon.text)
        print(jj)
        results = jj['result']
        identification = id_field
        try:
            phone = results["formatted_phone_number"]
        except KeyError:
            continue
        try:
            website = results["website"]
        except KeyError:
            continue
        title = results["name"]
        detailed_data = [title, identification, phone, website]
        final_detailed_data.append(detailed_data)

    columns = ["Business", "ID_Field","Phone", "Website"]
    details_df = pd.DataFrame.from_records(final_detailed_data, columns=columns)
    details_df.to_csv('further_details.csv')
    
    return details_df

def join_data(details_df,location_df):
    
    '''
    Joins the dfs together based on unique ID_Fields and write it to a final .CSV
    '''

    final_sheet = location_df.join(details_df.set_index('ID_Field'), on='ID_Field')

    final_sheet.to_csv("bakery.csv")

    print(final_sheet)

    return final_sheet

    
def main():
    #assigning Parameters for location searching
    api_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
    keywords = ['bakery']
    search_radius = '10000'
    coords = '45.415488,-75.697123'
    request_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location='+coords+'&radius='+str(search_radius)+'&keyword='+str(keywords)+'&key='+str(api_key)

    #find locations of desired establishments from google maps
    final_data, location_df = find_locations(request_url, api_key)

    #find website, phone number and ratings of establishments
    details_df = find_details(final_data, api_key)

    #join both dataframes together to have a final product
    non_spatial_data = join_data(details_df,location_df)

    #c
    spatial_df = csv_to_point(non_spatial_data)

    postgis_push(spatial_df)



if __name__ == "__main__":
    main()