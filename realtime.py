from datetime import datetime
import os
import json
import urllib.request


SNAPSHOTS_DIR = os.path.join(os.getcwd(), "snapshots")
GBFS_URL = "http://gbfs.citibikenyc.com/gbfs/gbfs.json"


def read_json(url):
    res = urllib.request.urlopen(url).read()
    return json.loads(res)


def write_json(dict_, file_name):
    with open(file_name, "w") as outfile:
        json.dump(dict_, outfile)


class RealTimeData:
    def __init__(
        self, 
        url=GBFS_URL, 
        data_dir=SNAPSHOTS_DIR,
        snapshot=None
    ):
        self.url = url
        self.data_dir = data_dir
        self.snapshot = snapshot
        
        self.language = 'en'
        
        self.last_update_time = None
        self.ttl = None
        self.saved = False

        self._url_map = None
        self._feeds = None
        self._snapshot_path = None
        self._datasets = None
        
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
    
    @property
    def snapshot_path(self):
        if self._snapshot_path is None:
            if self.snapshot is None:
                self.save()
            else:
                self._snapshot_path = os.path.join(
                    self.data_dir, str(self.snapshot)
                )
                if not os.path.exists(self._snapshot_path):
                    raise FileNotFoundError(f"No snapshot at {str(self.snapshot)}")
        return self._snapshot_path
    
    def save(self):
        dir_ = self.data_dir
        if self.snapshot is not None:
            self.snapshot = None
        snapshot = datetime.now().timestamp()
        self.snapshot = snapshot
        print(f"New snapshot at {str(snapshot)}.")

        save_dir = os.path.join(dir_, str(SNAPSHOTS_DIR))
        self.snapshot_path = save_dir
  
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)
        
        for name, url in self.feeds.items():
            data = read_json(url)
            write_json(data, os.path.join(save_dir, name+".json"))
        self.saved = True
    
    @property
    def datasets(self):
        if self._datasets is None:
            datasets = {
                'system_information': SystemInformation,
                'station_information': StationInformation,
                'station_status': StationStatus,
            }
            res = {}
            for name, class_name in datasets.items():
                res[name] = class_name(
                    url=self.url, 
                    snapshot=self.snapshot, 
                    data_dir=self.data_dir
                )
            self._datasets = res
        return self._datasets


class SystemInformation(RealTimeData):
    def __init__(self, url=GBFS_URL, **kwargs):

        self._data = None
        super().__init__(url=url, **kwargs)
    
    def update(self):
        if self.snapshot is not None:
            file_name = os.path.join(self.snapshot_path, 'system_information.json')
            with open(file_name) as f:
                res = json.load(f)
        else:
            super().update()
            url = self._feeds['system_information']
            res = read_json(url)
        self._data = res
        self.last_update_time = res['last_updated']
        self.ttl = res['ttl']
    
    @property
    def data(self):
        if self._data is None:
            self.update()
        return self._data
    

class StationInformation(RealTimeData):
    def __init__(self, url=GBFS_URL, **kwargs):
        
        self._data = None
        self._stations = None
        super().__init__(url=url, **kwargs)
    
    def update(self):
        if self.snapshot is not None:
            file_name = os.path.join(self.snapshot_path, 'station_information.json')
            with open(file_name) as f:
                res = json.load(f)
        else:
            super().update()
            url = self._feeds['station_information']
            res = read_json(url)
        self._data = res
        self._stations = res['data']['stations']
        self.last_update_time = res['last_updated']
        self.ttl = res['ttl']
    
    @property
    def data(self):
        if self._data is None:
            self.update()
        return self._data
    
    @property
    def stations(self):
        if self._stations is None:
            self.update()
        return self._stations


class StationStatus(RealTimeData):
    def __init__(self, url=GBFS_URL, **kwargs):
        
        self._data = None
        self._stations = None
        super().__init__(url=url, **kwargs)
    
    def update(self):
        if self.snapshot is not None:
            file_name = os.path.join(self.snapshot_path, 'station_status.json')
            with open(file_name) as f:
                res = json.load(f)
        else:
            super().update()
            url = self._feeds['station_status']
            res = read_json(url)
        self._data = res
        self._stations = res['data']['stations']
        self.last_update_time = res['last_updated']
        self.ttl = res['ttl']
    
    @property
    def data(self):
        if self._data is None:
            self.update()
        return self._data
    
    @property
    def stations(self):
        if self._stations is None:
            self.update()
        return self._stations