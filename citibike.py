import pandas as pd
from datetime import datetime
import os
import json
import urllib.request
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from zipfile import ZipFile, BadZipfile
import io


DATA_DIR = os.path.join(os.getcwd(), "trip_data", "raw")
GBFS_URL = "http://gbfs.citibikenyc.com/gbfs/gbfs.json"
TRIPDATA_URL = "https://s3.amazonaws.com/tripdata"


def time_buckets(df, key, freq='1min', offset=0, label='left'):
    return df.sort_values(key).groupby(
        pd.Grouper(key=key, freq=freq, offset=offset, label=label)
    )


def read_json(url):
    res = urllib.request.urlopen(url).read()
    return json.loads(res)


def read_zip_file(url):
    response = requests.get(url, stream=True, verify=False)
    with ZipFile(io.BytesIO(response.content)) as myzip:
        with myzip.open(myzip.namelist()[0]) as myfile:
            return pd.read_csv(myfile)
    

class Station:
    def __init__(self, trip_data, name):
        
        self.trip_data = trip_data
        self.name = name
        self.id = None
        
        self._departures = None
        self._arrivals = None
        self._coordinates = None
        self._info = None
        self._status = None
    
    @property
    def departures(self):
        if self._departures is None:
            self._departures = self.trip_data.data.query(f"start_station_name == '{self.name}'")
        return self._departures
    
    @property
    def arrivals(self):
        if self._arrivals is None:
            self._arrivals = self.trip_data.data.query(f"end_station_name == '{self.name}'")
        return self._arrivals
    
    @property
    def coordinates(self):
        if self._coordinates is None:
            td = self.trip_data
            data = td.data[td.data['start_station_name'] == self.name].iloc[0]
            self._coordinates = (data['start_lat'], data['start_lng'])
        return self._coordinates
    
    @property
    def info(self):
        if self._info is None:
            all_stations = StationInformation().stations
            self._info = [i for i in all_stations if i['name'] == self.name][0]
            self.id = self._info['station_id']
        return self._info
    
    @property
    def status(self):
        if self._status is None:
            all_stations = StationStatus().stations
            self._status = [i for i in all_stations if i['station_id'] == self.id][0]
        return self._status
            
    def trip_counts(self, freq='1min'):
        res = pd.DataFrame({
            "departure": time_buckets(self.departures, 'started_at', freq=freq)['ride_id'].count(),
            "arrival": time_buckets(self.arrivals, 'ended_at', freq=freq)['ride_id'].count()
        })
        res = res.reindex(self.trip_data.time_range(freq=freq))
        res = res.fillna(0)
        res = res.unstack().reset_index(level=0)
        res.columns = ['type', 'count']
        return res
    
    def est_bike_availability(self, freq='1min', **kwargs):
        trip_counts = self.trip_counts(freq=freq).query("type == 'arrival'")
        return trip_counts['count'].ewm(**kwargs).mean()
    
    def est_dock_availability(self, freq='1min', **kwargs):
        trip_counts = self.trip_counts(freq=freq).query("type == 'departure'")
        return trip_counts['count'].ewm(**kwargs).mean()


class TripData:
    def __init__(self, month: str, data_dir=DATA_DIR, trip_data_url=TRIPDATA_URL):
        self.file_name = f"{str(month)}-citibike-tripdata.csv"
        self.file_path = os.path.join(data_dir, self.file_name)
        self.start = datetime(int(month[:4]), int(month[4:]), 1)
        self.end = self.start + pd.offsets.DateOffset(months=1)
        self.trip_data_url = trip_data_url
        
        self._data = None
        self._station_names = None
        self._stations = None
        
    def download(self, save=True):
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        url = os.path.join(self.trip_data_url, self.file_name + ".zip")
        url = f"{self.trip_data_url}/{self.file_name}.zip"
        
        # handling typos in url names
        try:
            self._data = read_zip_file(url)
        except: # BadZipFile 
            url = url.replace("-citibike-tripdata.csv", "-citbike-tripdata.csv")
            try:
                self._data = read_zip_file(url)
            except: #BadZipFile
                url = url.replace("-citbike-tripdata.csv", "-citbike-tripdata")
                self._data = read_zip_file(url)
            
        if save:
            self._data.to_csv(self.file_path)
        print("Finished.")
        
    def get_data(self, download=True):
        
        try:
            data = pd.read_csv(self.file_path)
        except FileNotFoundError:
            if download:
                self.download(save=True)
                data = pd.read_csv(self.file_path)
            else:
                raise(FileNotFoundError)
                
        data.loc[:, 'started_at'] = pd.to_datetime(data.loc[:, 'started_at'])
        data.loc[:, 'ended_at'] = pd.to_datetime(data.loc[:, 'ended_at'])
        self._data = data
    
    def get_station_names(self):
        
        station_names = self.data[
            ['start_station_name']
        ].drop_duplicates()['start_station_name']
        self._station_names = list(station_names)
        
    @property
    def data(self):
        if self._data is None:
            self.get_data(download=True)
        return self._data
    
    @property
    def station_names(self):
        if self._station_names is None:
            self.get_station_names()
        return self._station_names
    
    @property
    def stations(self):
        if self._stations is None:
            self._stations = [Station(trip_data=self, name=k) for k in self.station_names]
        return self._stations
    
    def time_range(self, freq='1min'):
        return pd.date_range(self.start, self.end, freq=freq)[:-1]
    

class RealTimeData:
    def __init__(self, url=GBFS_URL):
        self.url = url
        self.language = 'en'
        
        self.last_update_time = None
        self.ttl = None
        
        self._url_map = None
        self._feeds = None
        
    def update(self):
        res = read_json(self.url)
        
        self.last_update_time = res['last_updated']
        self.ttl = res['ttl']
        
        self._url_map = res['data']
        self._feeds = {i['name']: i['url'] for i in res['data'][self.language]['feeds']}
    
    @property
    def url_map(self):
        if self._url_map is None:
            self.update()
        return self._url_map
    
    @property
    def feeds(self):
        if self._feeds is None:
            self.update()
        return self._feeds
    
    
class StationInformation(RealTimeData):
    def __init__(self, url=GBFS_URL):
        
        self._stations = None
        super().__init__(url=url)
    
    def update(self):
        super().update()
        url = self._feeds['station_information']
        res = read_json(url)
        self._stations = res['data']['stations']
        self.last_update_time = res['last_updated']
        self.ttl = res['ttl']
    
    @property
    def stations(self):
        if self._stations is None:
            self.update()
        return self._stations


class StationStatus(RealTimeData):
    def __init__(self, url=GBFS_URL):
        
        self._stations = None
        super().__init__(url=url)
    
    def update(self):
        super().update()
        url = self._feeds['station_status']
        res = read_json(url)
        self._stations = res['data']['stations']
        self.last_update_time = res['last_updated']
        self.ttl = res['ttl']
    
    @property
    def stations(self):
        if self._stations is None:
            self.update()
        return self._stations
        
        
    
    
        
        
    
    
    
    
        
        
    
    
    
    