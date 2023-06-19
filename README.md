# Utility Classes for Real-Time and Historical Citibike Data

This repository contains Python scripts that download and analyze data from the Citibike bike-sharing system in New York City. The scripts provide a way to interact with both the real-time data provided by Citibike's GBFS (General Bikeshare Feed Specification) [feed](http://gbfs.citibikenyc.com/gbfs/gbfs.json), as well as the [historical trip data](https://s3.amazonaws.com/tripdata/index.html) provided by Citibike's public data site. More details on these datasets can be found [here](https://ride.citibikenyc.com/system-data). 

## Files in this Repository

- **citibike.py**: This file contains the Station class, which represents a Citibike station. This class provides methods to query for trips departing from or arriving at the station, as well as estimate bike and dock availability.
- **historical.py**: This file contains the TripData class, which represents a month's worth of trip data. This class provides methods to download the data, read it into a pandas DataFrame, and perform some preliminary cleaning and processing. It also provides a method to return a list of Station objects representing all stations that appear in the trip data.
- **realtime.py**: This file contains several classes that interact with the Citibike GBFS feed, including classes for system information, station information, and station status.

## How to Use

1. **Clone this repository**
You can clone this repository using git command:
```
git clone https://github.com/tracy-stephens/citibike.git
```
2. **Install Dependencies**
Make sure you have the necessary Python packages installed on your system. If you don't have them installed, you can install them using pip:
```
pip install pandas requests urllib3
```
3. **Run Scripts**
The scripts can be run directly from a Python interpreter or used as modules in other Python programs.
For example, to download a month's worth of trip data, create a TripData object with the month as an argument and call its download method:
```
from historical import TripData
td = TripData('202205')
td.download()
```
To get a list of all stations in the data, call the stations property:
```
stations = td.stations
```
To get data about a specific station, use the find_station method:
```
station = td.find_station('8 Ave & W 31 St')
```

## Limitations
- The analysis is currently limited to New York City's Citibike system.
- The scripts make use of the real-time Citibike GBFS feed, but the data from the feed is not updated in real time.

## Contributing
Feel free to submit a pull request if you want to make improvements or add new features.

## License
This project is licensed under the terms of the MIT license.
