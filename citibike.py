import pandas as pd
from realtime import StationInformation, StationStatus


def time_buckets(df, key, freq='1min', offset=0, label='left'):
    return df.sort_values(key).groupby(
        pd.Grouper(key=key, freq=freq, offset=offset, label=label)
    )


class Station:
    def __init__(self, trip_data, name: str, snapshot=None):
        
        self.trip_data = trip_data
        self.name = name
        self.id = None
        self.snapshot = snapshot
        
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
            all_stations = StationInformation(snapshot=self.snapshot).stations
            try:
                self._info = [
                    i for i in all_stations if i['name'].replace('\t', 't') == self.name.replace('\\','')
                ][0]
                self.id = self._info['station_id']
            except IndexError:
                print(f'No station info found for {self.name}')
                return 
        return self._info
    
    @property
    def status(self):
        if self.id is None:
            self.id = self.info['station_id']
        if self._status is None:
            all_stations = StationStatus(snapshot=self.snapshot).stations
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
