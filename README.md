# Disaster_Risk-Assessment-Project


So far as of 11/18/24:

	Cleaned data: got rid of some columns, got rid of the time stamps in dates
	Replaced Nans in Longitude and Latitude fields w/ 0s
	Created markers in Folio 
Issues:
	Can't backup repo as the raw data file is too big - change file to text csv not Excel csv?
	Too many markers for folio to generate a map - need to categorize by zip?  

requirements for ve
	folium
	pandas
	webbrowser
	numpy
	duckdb


Virtual Environment Commands
| Command | Linux/Mac | GitBash |
| ------- | --------- | ------- |
| Create | `python3 -m venv venv` | `python -m venv venv` |
| Activate | `source venv/bin/activate` | `source venv/Scripts/activate` | 
| Install | `pip install -r requirements.txt` | `pip install -r requirements.txt` |
| Deactivate | `deactivate` | `deactivate` |sour