import pandas as pd
from datetime import datetime
import os
import requests
import urllib3
from zipfile import ZipFile, BadZipfile
import io

from citibike import Station


TRIP_DATA_DIR = os.path.join(os.getcwd(), "data", "trip_data")
TRIPDATA_URL = "https://s3.amazonaws.com/tripdata"

    
def read_zip_file(url):
    response = requests.get(url, stream=True, verify=False)
    with ZipFile(io.BytesIO(response.content)) as myzip:
        with myzip.open(myzip.namelist()[0]) as myfile:
            return pd.read_csv(myfile)
    

class TripData:
    def __init__(
        self, 
        month: str, 
        data_dir=TRIP_DATA_DIR, 
        trip_data_url=TRIPDATA_URL,
        snapshot=None
    ):
        self.file_name = f"{str(month)}-citibike-tripdata.csv"
        self.file_path = os.path.join(data_dir, self.file_name)
        self.start = datetime(int(month[:4]), int(month[4:]), 1)
        self.end = self.start + pd.offsets.DateOffset(months=1)
        self.trip_data_url = trip_data_url
        self.snapshot = snapshot
        
        self._data = None
        self._station_names = None
        self._stations = None
        
    def download(self, save=True):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
            data = pd.read_csv(self.file_path, low_memory=False)
        except FileNotFoundError:
            if download:
                self.download(save=True)
                data = pd.read_csv(self.file_path, low_memory=False)
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
            self._stations = [
                Station(
                    trip_data=self, name=k, snapshot=self.snapshot
                ) for k in self.station_names
            ]
        return self._stations

    def find_station(self, station_name):
        stations = self.stations
        res = [i for i in stations if i.name == station_name][0]
        return res
    
    def time_range(self, freq='1min'):
        return pd.date_range(self.start, self.end, freq=freq)[:-1]
    