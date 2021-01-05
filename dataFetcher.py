#!/usr/bin/env python
# coding: utf-8

# # Loading data

# In[8]:


import pandas as pd
import plotly.express as px
from tqdm import tqdm
import functools
import numpy as np
from difflib import SequenceMatcher
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from datetime import datetime, timedelta
import pprint
import requests
import os
import getpass
import json


# In[9]:


#cashing in case of multiple calls.
@functools.lru_cache(maxsize=128)
def get_tiles(municipalityId: int) -> pd.DataFrame:
    """Fetches tile information for a municipality id.
    
    Args:
        municipalityId: id of the municipality as defined in by the federal office of statistics,
            https://www.bfs.admin.ch/bfs/fr/home/bases-statistiques/repertoire-officiel-communes-suisse.assetdetail.11467406.html
            
    Return:
        A dataframe containing the following columns:
        [tileId, ll_lon, ll_lat, urL-lon, ur_lat]
        
        tileID: corresponds to a unique ID as defined in the Swisscom FAQ page.
        ll_lon: longitude coordinate of the lower left corner of the tile.
        ll_lat: latitude coordinate of the lower left corner of the tile.
        ur_lon: longitude coordinate of the upper right corner of the tile.
        ur_lat: latitude coordinate of the upper right corner of the tile.
        
        If municipalityId is invalid will print an error message and return an empty DataFrame
    """
    api_request = (
        BASE_URL
        + f'/grids/municipalities/{municipalityId}'
    )

    data = oauth.get(api_request, headers=headers).json()
    if(data.get('status') == None):
        tileID = [t['tileId'] for t in data['tiles']]
        ll_lon = [t['ll']['x'] for t in data['tiles']]
        ll_lat= [t['ll']['y'] for t in data['tiles']]
        ur_lon = [t['ur']['x'] for t in data['tiles']]
        ur_lat = [t['ur']['y'] for t in data['tiles']]
    else:
        print(f'get_tiles: failed with status code {data.get("status")}. {data.get("message")}')
        return pd.DataFrame(data={'tileID': [], 'll_lat': [], 'll_lon': [], 'ur_lat': [], 'ur_lon': []})
    
    return pd.DataFrame(data={'tileID': tileID, 'll_lat': ll_lat, 'll_lon': ll_lon, 'ur_lat': ur_lat, 'ur_lon': ur_lon})


# In[10]:


def get_municipalityID(name: str) -> np.array(int):
    """Converts a municipality name to ID
    
    Args:
        name of municipality.
    
    Returns:
        An array containing all the municipality ID's corresponding to the name.
        
        If the name invalid will return an empty array.
    """
    return commune.loc[commune.GDENAME == name].GDENR.to_numpy()


# In[11]:


def visualize_coordinates(df: pd.DataFrame, latitude: str, longitude: str) -> None :
    """Visualizes coordinates in dataframe on map
    
    Retrieves columns with name latitude and logitude and visualizes it on a map.
    
    Args:
        df: A dataframe containing the coordinates.
        latitude: String key of the column in the dataframe containing the latitude.
        longitude: String key of the column in the dataframe containing the longitude.
    """
    fig = px.scatter_mapbox(df, lat=latitude, lon=longitude,
                  color_continuous_scale=px.colors.cyclical.IceFire, size_max=15, zoom=10,
                  mapbox_style="carto-positron")
    fig.show()


# In[12]:


def get_all_tiles_switzerland() -> pd.DataFrame:
    """Fetches the tile information for all the tiles in Switzerland.
    
    Returns:
        A Dataframe containg the tile information for every tile in switzerland.
        
        The format of the DataFrame is the same as the return of get_tiles()
    
    """
    tiles = get_tiles(commune.GDENR.unique()[0])
    for c in tqdm(commune.GDENR.unique().tolist()):
        tiles = tiles.append(get_tiles(c))
    return tiles



# In[37]:


def get_daily_demographics(tiles, day=datetime(year=2020, month=1, day=27, hour=0, minute=0) ):
    """Fetches daily demographics
    
    Fetches the daily demographics, age distribution, of the tiles.
    
    Args:
        tiles: Array of tile id's, what will be used to querry demographic data.
        day: date of the data to be fetched.
        
    Returns:
        A dataframe containing as a key the tileID and as columns ageDistribution and the maleProportion
        
        +----------+-----------------------+---------------------+
        |          | ageDistribution       | maleProportion      |
        +----------+-----------------------+---------------------+
        | 44554639 | NaN                   | 0.49828359484672546 |
        +----------+-----------------------+---------------------+
        | 44271906 | [0.21413850784301758, | 0.493218            |
        |          |  0.27691012620925903, |                     |
        |          |  0.37422287464141846, |                     |
        |          |  0.13472850620746613] |                     |
        +----------+-----------------------+---------------------+
        In the example above tile 44554639 does not have any age distribution data.
        
        The data is k-anonymized. Therefor is some tiles are missing data it
        means that the data is not available. To find out more about demographics visit the Heatmap FAQ.
    """
    dates = [(day + timedelta(hours=delta)) for delta in range(24)]
    date2score = dict()
    for tiles_subset in [tiles[i:i + MAX_NB_TILES_REQUEST] for i in range(0, len(tiles), MAX_NB_TILES_REQUEST)]:
            api_request = (
                BASE_URL
                + f'/heatmaps/dwell-demographics/daily/{day.isoformat().split("T")[0]}'
                + "?tiles="
                + "&tiles=".join(map(str, tiles_subset))
            )
            data = oauth.get(api_request, headers=headers).json()
            for t in data.get("tiles", []):
                if date2score.get(t['tileId']) == None:
                    date2score[t['tileId']] = dict()
                date2score[t['tileId']] = {"ageDistribution": t.get("ageDistribution"),"maleProportion": t.get("maleProportion")}
    
    
    return pd.DataFrame.from_dict(date2score).transpose()



# In[48]:


def get_hourly_demographics_dataframe(tiles, day=datetime(year=2020, month=1, day=27, hour=0, minute=0)):
    """Fetches hourly demographics of age categories for 24 hours
    
    Fetches the hourly demographics, age distribution, of the tiles.
    
    Age categories are the following 0 - 19, 20 - 39, 40 - 64, >64
    
    Args:
        tiles: Array of tile id's, what will be used to querry demographic data.
        day: date of the data to be fetched.
        
    Returns:
        DataFrame containing the demographics. The name
        of the collumns are:
            [age_cat, age_distribution, male_proportion]
            
        +----------+---------------------+---------+------------------+-----------------+
        |          |                     | age_cat | age_distribution | male_proportion |
        +----------+---------------------+---------+------------------+-----------------+
        | tileID   | time                |         |                  |                 |
        +----------+---------------------+---------+------------------+-----------------+
        | 44394309 | 2020-01-27T00:00:00 | NaN     | NaN              | 0.474876        |
        +----------+---------------------+---------+------------------+-----------------+
        |          | 2020-01-27T01:00:00 | NaN     | NaN              | 0.483166        |
        +----------+---------------------+---------+------------------+-----------------+
        |          | ...                 |         |                  |                 |
        +----------+---------------------+---------+------------------+-----------------+
        | 44290729 | 2020-01-27T06:00:00 | 0.0     | 0.192352         | 0.497038        |
        +----------+---------------------+---------+------------------+-----------------+
        |          | 2020-01-27T06:00:00 | 1.0     | 0.269984         | 0.497038        |
        +----------+---------------------+---------+------------------+-----------------+
        |          | 2020-01-27T06:00:00 | 2.0     | 0.363481         | 0.497038        |
        +----------+---------------------+---------+------------------+-----------------+
        |          | 2020-01-27T06:00:00 | 3.0     | 0.174183         | 0.497038        |
        +----------+---------------------+---------+------------------+-----------------+
        
        The data is k-anonymized. Therefor is some tiles are not present in the output dataframe it 
        means that the data is not available. To find out more about demographics visit the Heatmap FAQ.
    """
    def get_hourly_demographics(tiles, day=datetime(year=2020, month=1, day=27, hour=0, minute=0) ):
        """Fetches hourly male proportion and age categories for 24 hours

        Args:
            tiles: Array of tile id's, what will be used to querry demographic data.
            day: date of the data to be fetched.

        Returns:
            Returns a dictionary with as a key the tileID, and as a value an object that is as follows:

            {tileID: {dateTime:{ "ageDistribution": [0-19, 20-39, 40-64, 64+], "maleProportion": value},
                     {dateTime2: ...}}}



            26994514: {'2020-01-27T00:00:00': {'ageDistribution': [0.1925136297941208,
            0.2758632302284241,
            0.362215131521225,
            0.16940800845623016],
           'maleProportion': 0.4727686941623688},
           '2020-01-27T01:00:00': {'ageDistribution': None,
           'maleProportion': 0.4896690547466278},
           '2020-01-27T02:00:00': {'ageDistribution': None,
           'maleProportion': 0.48882684111595154},

            The data is k-anonymized. Therefor is some values are None it means that no data was available 
            To find out more about demographics visit the Heatmap FAQ.
        """
        dates = [(day + timedelta(hours=delta)) for delta in range(24)]
        date2score = dict()
        for dt in tqdm(dates, desc="get_hourly_demographics: hours", leave=True):
            for tiles_subset in [tiles[i:i + 100] for i in range(0, len(tiles), 100)]:
                api_request = (
                    BASE_URL
                    + f'/heatmaps/dwell-demographics/hourly/{dt.isoformat()}'
                    + "?tiles="
                    + "&tiles=".join(map(str, tiles_subset))
                )
                data = oauth.get(api_request, headers=headers).json()
                for t in data.get("tiles", []):
                    if date2score.get(t['tileId']) == None:
                        date2score[t['tileId']] = dict()
                    date2score.get(t['tileId'])[dt.isoformat()] = {"ageDistribution": t.get("ageDistribution"),"maleProportion": t.get("maleProportion")}
        return date2score
    
    
    
    data = get_hourly_demographics(tiles, day)
    tile_id = []
    time_data = []
    age_distribution = []
    age_cat = []
    male_proportion = []
    for i in data:
        for time in data[i]:
            if data[i][time].get("ageDistribution") != None:
                for (idx,a) in enumerate(data[i][time].get("ageDistribution", [])):
                    age_cat.append(idx)
                    age_distribution.append(a)
                    tile_id.append(i)
                    time_data.append(time)
                    male_proportion.append(data[i][time].get("maleProportion"))
            else:
                tile_id.append(i)
                time_data.append(time)
                age_distribution.append(None)
                male_proportion.append(data[i][time].get("maleProportion"))
                age_cat.append(None)
    return pd.DataFrame(data={'tileID': tile_id, "age_cat": age_cat, 'age_distribution':age_distribution, "male_proportion": male_proportion, 'time': time_data}).set_index(['tileID', 'time'])


# In[65]:


def get_daily_density(tiles: np.array(int), day=datetime(year=2020, month=1, day=27)) -> pd.DataFrame:
    """Fetches the daily density of tiles.
    
    Fetches the daily density of the tiles and creates a dataframe of the fetched data.
    
    Args:
        tiles: Array of tile id's that daily density data needs to be fetched.
        day: Day to fetch the density data for.
        
    Returns:
        DataFrame containg the tileId and the score. The name of the collumns are:
            [score]
        
        The identifier of the row is bassed on the tileID
        
        +----------+-------+
        |          | score |
        +----------+-------+
        |  tileID  |       |
        +----------+-------+
        | 44394309 | 1351  |
        +----------+-------+
        | 44394315 | 1103  |
        +----------+-------+
        | 44460297 | 875   |
        +----------+-------+
        | 44488589 | 1387  |
        +----------+-------+
        | 44498028 | 678   |
        +----------+-------+
        
        Tile with k-anonymized dwell density score. If tile not present Swisscom is
        unable to provide a value due to k-anonymization. To find out more on density
        scores read the Heatmap FAQ.   
    """
    tileID = []
    score = []
    for tiles_subset in [tiles[i:i + MAX_NB_TILES_REQUEST] for i in range(0, len(tiles), MAX_NB_TILES_REQUEST)]:
            api_request = (
                BASE_URL
                + f'/heatmaps/dwell-density/daily/{day.isoformat().split("T")[0]}'
                + "?tiles="
                + "&tiles=".join(map(str, tiles_subset))
            )
            data = oauth.get(api_request, headers=headers).json()
            if data.get("tiles") != None:
                for t in data["tiles"]:
                    tileID.append(t['tileId'])
                    score.append(t["score"])
    return pd.DataFrame(data={'tileID': tileID, 'score':score}).set_index("tileID")


# In[66]:


def get_hourly_density_dataframe(tiles, day=datetime(year=2020, month=1, day=27, hour=0, minute=0)):
    """Fetches the hourly density of tiles for 24 hours.

        Fetches the hourly density of the tiles and creates a dataframe of the fetched data.

        Args:
            tiles: Array of tile id's that daily density data needs to be fetched.
            day: Day to fetch the density data for.

        Returns:
            DataFrame containg the tileId and the score. The name of the collumns are:
                [score]
            The identifier of the row is bassed on the [tileID, time]
            
            +----------+---------------------+-------+
            |          |                     | score |
            +----------+---------------------+-------+
            |  tileID  |         time        |       |
            +----------+---------------------+-------+
            | 44394309 | 2020-01-27T00:00:00 | 52    |
            |          +---------------------+-------+
            |          | 2020-01-27T01:00:00 | 68    |
            |          +---------------------+-------+
            |          | 2020-01-27T02:00:00 | 69    |
            |          +---------------------+-------+
            |          | 2020-01-27T03:00:00 | 69    |
            |          +---------------------+-------+
            |          | 2020-01-27T04:00:00 | 69    |
            +----------+---------------------+-------+

            Tile with k-anonymized dwell density score. If tile not present Swisscom is
            unable to provide a value due to k-anonymization. To find out more on density
            scores read the Heatmap FAQ.   
    """
    
    def get_hourly_density(tiles, day=datetime(year=2020, month=1, day=27, hour=0, minute=0)):
        dates = [(day + timedelta(hours=delta)) for delta in range(24)]
        date2score = dict()
        print("getHourlyDensity")
        for dt in tqdm(dates, desc="get_hourly_density: hours", leave=True):
            for tiles_subset in [tiles[i:i + 100] for i in range(0, len(tiles), 100)]:
                api_request = (
                    BASE_URL
                    + f'/heatmaps/dwell-density/hourly/{dt.isoformat()}'
                    + "?tiles="
                    + "&tiles=".join(map(str, tiles_subset))
                )
                for t in oauth.get(api_request, headers=headers).json().get("tiles",[]):
                    if date2score.get(t['tileId']) == None:
                        date2score[t['tileId']] = dict()
                    date2score.get(t['tileId'])[dt.isoformat()] = t['score']

        return date2score
    
    
    tiles_data = []
    time_data = []
    score = []
    data = get_hourly_density(tiles, day)
    for t in data:
        for time in data[t]:
            time_data.append(time)
            tiles_data.append(t)
            score.append(data[t][time])
    return pd.DataFrame(data={'tileID': tiles_data, 'score':score, 'time': time_data}).set_index(['tileID', 'time'])


# In[10]:


def fetch_data_city(city: str) -> None:
    """Fetches the data for a city if the data is not yet cashed on the computer.
    """
    compression = ".xz"
    folder = os.path.join(".","data")
    def file_path(file_name: str) -> str:
        return os.path.join(folder, file_name)

    if not(os.path.exists(folder)):
        os.mkdir(folder)
    
    
    tiles_path = file_path(f'{city}Tiles.pkl{compression}')
    hourly_dem_path = file_path(f'{city}HourlyDemographics.pkl{compression}')
    hourly_density_path = file_path(f'{city}HourlyDensity.pkl{compression}')
    daily_density_path = file_path(f'{city}DensityDaily.pkl{compression}')
    daily_demographics_path = file_path(f'{city}DemographicsDaily.pkl{compression}')


    if not(os.path.isfile(tiles_path)):
        tiles = get_tiles(get_municipalityID(city)[0])
        tiles.to_pickle(tiles_path)
    else:
        tiles = pd.read_pickle(tiles_path)
    if not(os.path.isfile(hourly_dem_path)):
        hourly_dem = get_hourly_demographics_dataframe(tiles['tileID'].to_numpy())
        hourly_dem.to_pickle(hourly_dem_path)
    if not(os.path.isfile(hourly_density_path)):
        hourly_dens = get_hourly_density_dataframe(tiles['tileID'].to_numpy())
        hourly_dens.to_pickle(hourly_density_path)
    if not(os.path.isfile(daily_density_path)):
        get_daily_density(tiles['tileID'].to_numpy()).to_pickle(daily_density_path)
    if not(os.path.isfile(daily_demographics_path)):
        get_daily_demographics(tiles['tileID'].to_numpy()).to_pickle(daily_demographics_path)






# In[11]:


def clean_cities_list(cities: [str]) -> [str]:
    """Cleans the list of cities by removing all the cities that are not found in the 
    official list of cities provided by the Federal Statisitics Office.
    
    Args:
        List of cities to check and clean.
    
    Return:
        List containing a subset of the input list such that all elements are valid.
    """
    invalid_cities = []
    #validation that the cities names are valid
    for c in cities:
        if len(commune.loc[commune.GDENAME == c].GDENR.to_numpy()) == 0:
            city = []
            sim_value = []
            for f in commune.GDENAME:
                r = SequenceMatcher(None, c, f).ratio()
                if r > 0.5:
                    city.append(f)
                    sim_value.append(r)

            d = pd.DataFrame(data={"city": city, "value": sim_value})
            
            potential_cities = d.sort_values("value", ascending=False).head(5).city.to_numpy()
            print(f"City nammed: {c} cannot be found in official records. Did you mean: {potential_cities} ? {c} will be ignored.")
            invalid_cities.append(c)
    return [c for c in cities if not(c in invalid_cities)]


# Multithread fetch implementation

# In[13]:


from queue import Queue
from threading import Thread
from time import time
import logging
import os


# In[14]:


class DownloadWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            city = self.queue.get()
            if city == -1:
                self.queue.put(-1)
                break
            try:
                fetch_data_city(city)
            finally:
                self.queue.task_done()


# In[15]:


def download_commune_excel() -> None:
    '''
    Downloads the excel spreadsheet from the Swiss Federal Statistical Office that maps the town name to unique ID
    '''
    
    print('Beginning commune file download with requests')

    url = 'https://www.bfs.admin.ch/bfsstatic/dam/assets/11467406/master'
    r = requests.get(url)

    with open(os.path.join(".", "data", 'commune.xlsx'), 'wb') as f:
        f.write(r.content)
    print("End of commune file download")
    


# In[23]:


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL = "https://api.swisscom.com/layer/heatmaps/demo"
TOKEN_URL = "https://consent.swisscom.com/o/oauth2/token"
MAX_NB_TILES_REQUEST = 100
headers = {"scs-version": "2"}
client_id = ""  # customer key in the Swisscom digital market place
client_secret = ""  # customer secret in the Swisscom digital market place

if client_id == "":
    client_id = os.environ.get("CLIENT_ID", "")
    if client_id == "":
        client_id = input("Enter MIP Client ID: ")
    os.environ["CLIENT_ID"] = client_id
if client_secret == "":
    client_secret = os.environ.get("CLIENT_SECRET", "")
    if client_secret == "":
        client_secret = getpass.getpass('Enter MIP client secret:')
    os.environ["CLIENT_SECRET"] = client_secret

# Fetch an access token
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)
oauth.fetch_token(token_url=TOKEN_URL, client_id=client_id,
                client_secret=client_secret)


def main():
    ts = time()

    if not(os.path.exists(os.path.join(".", "data", 'commune.xlsx'))):
        download_commune_excel()
    global commune
    commune = pd.read_excel(os.path.join(".", "data", 'commune.xlsx'), sheet_name='GDE')
    
    cities = ["Saas-Fee", "Evolène", "Arosa", "Bulle"]#, "Laax","Belp" ,"Saanen","Adelboden", "Andermatt", "Davos", "Bulle", "Bern", "Genève", "Lausanne", "Zurich", "Neuchâtel", "Sion", "St. Gallen", "Appenzell", "Solothurn", "Zug", "Fribourg", "Luzern", "Ecublens (VD)", "Kloten", "Le Grand-Saconnex", "Nyon", "Zermatt", "Lugano", "Romont"]
    cities = clean_cities_list(cities)
    queue = Queue()
    for x in range(2):
        worker = DownloadWorker(queue)
        worker.deamen = True
        worker.start()
    for c in cities:
        logger.info('Queueing {}'.format(c))
        queue.put(c)
    queue.join()

    queue.put(-1)
    logger.info('Took %s', time() - ts)


    list_of_cities_path = os.path.join(".", "data","CityList.json")
    cityList=[]
    if os.path.isfile(list_of_cities_path):
        with open(list_of_cities_path, "r") as filehandle:
            cityList = json.load(filehandle)
    with open(list_of_cities_path, "w") as filehandle:
        for city in cities:
            if not(city in cityList):
                cityList.append(city)
        json.dump(cityList, filehandle)
    
    
if __name__ == "__main__":
    main()


# ## Other functions not currently used

# In[ ]:


def get_daily_demographics_male(tiles: np.array(int), day=datetime(year=2020, month=1, day=27)) -> pd.DataFrame:
    """Fetches Daily demographics.
    
    Fetches the daily male proportion of the tiles and creates a dataframe of the fetched data.
    
    Args:
        tiles: Array of tile id's, what will be used to querry demographic data.
        day: date of the data to be fetched.
    
    Returns:
        DataFrame containing the tileId and the proportion of male. The name of the collumns are:
            [tileID, maleProportion]
        The data is k-anonymized. Therefor is some tiles are not present in the output dataframe it 
        means that the data is not available. To find out more about demographics visit the Heatmap FAQ.
    """
    
    tileID = []
    maleProportion = []

    for tiles_subset in [tiles[i:i + MAX_NB_TILES_REQUEST] for i in range(0, len(tiles), MAX_NB_TILES_REQUEST)]:
            api_request = (
                BASE_URL
                + f'/heatmaps/dwell-demographics/daily/{day.isoformat().split("T")[0]}'
                + "?tiles="
                + "&tiles=".join(map(str, tiles_subset))
            )
            data = oauth.get(api_request, headers=headers).json()
            if data.get("tiles") != None:
                for t in data["tiles"]:
                    if t.get("maleProportion") != None:
                        tileID.append(t['tileId'])
                        maleProportion.append(t["maleProportion"])
    return pd.DataFrame(data={'tileID': tileID, 'maleProportion':maleProportion})


# In[ ]:


def get_daily_demographics_age(tiles: np.array(int), day=datetime(year=2020, month=1, day=27)) -> pd.DataFrame:
    """Fetches daily demographics of age categories
    
    Fetches the daily demographics, age distribution, of the tiles and creates a dataframe of the fetched data.
    
    Args:
        tiles: Array of tile id's, what will be used to querry demographic data.
        day: date of the data to be fetched.
        
    Returns:
        DataFrame containing the tileId and a array of values corresponding to the age distribution. The name
        of the collumns are:
            [tileID, ageDistribution]
        The data is k-anonymized. Therefor is some tiles are not present in the output dataframe it 
        means that the data is not available. To find out more about demographics visit the Heatmap FAQ.
    """
    tileID = []
    ageDistribution = []
    
    for tiles_subset in [tiles[i:i + MAX_NB_TILES_REQUEST] for i in range(0, len(tiles), MAX_NB_TILES_REQUEST)]:
            api_request = (
                BASE_URL
                + f'/heatmaps/dwell-demographics/daily/{day.isoformat().split("T")[0]}'
                + "?tiles="
                + "&tiles=".join(map(str, tiles_subset))
            )
            data = oauth.get(api_request, headers=headers).json()
            for t in data.get("tiles", []):
                if t.get("ageDistribution") != None:
                    tileID.append(t['tileId'])
                    ageDistribution.append(t["ageDistribution"])
    return pd.DataFrame(data={'tileID': tileID, 'ageDistribution':ageDistribution})

