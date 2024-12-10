import folium
import pandas as pd
import numpy as np
import webbrowser
import duckdb
import gdown 

url  ="https://drive.google.com/file/d/1_2aV0idVYIhufdc-V9A7tZ_xhrqy4OcB/view?usp=drive_link" #Link to the Multiple Loss Properties File w/ GIS from 1977-2024
output = "data/multipleLossProperties2.csv"
gdown.download(url, output, fuzzy=True)

url1  ="https://drive.google.com/file/d/1_6hbi3LNp56q4oIl0DfyXF2HzNIZt-lp/view?usp=drive_link" #Link to combined US Census Total Dwellings Data from 2010-2022
output = "data/censusdata.csv"
gdown.download(url1, output, fuzzy=True)

#Data Cleaning

nfip_loss_r = pd.read_csv("data/multipleLossProperties2.csv", index_col=0) 

nfip_loss_r['asOfDate - Effective date on the File'] = pd.to_datetime(nfip_loss_r['asOfDate - Effective date on the File'])
nfip_loss_r["asOfDate - Effective date on the File"] = nfip_loss_r['asOfDate - Effective date on the File'].dt.date


nfip_loss_r['originalConstructionDate'] = pd.to_datetime(nfip_loss_r['originalConstructionDate'])
nfip_loss_r["originalConstructionDate"] = nfip_loss_r['originalConstructionDate'].dt.date

nfip_loss_r['originalNBDate - Date of the Beginning of the Flood Policy'] = pd.to_datetime(nfip_loss_r['originalNBDate - Date of the Beginning of the Flood Policy'])
nfip_loss_r["originalNBDate - Date of the Beginning of the Flood Policy"] = nfip_loss_r['originalNBDate - Date of the Beginning of the Flood Policy'].dt.date


nfip_loss_r['mostRecentDateofLoss'] = pd.to_datetime(nfip_loss_r['mostRecentDateofLoss'])
nfip_loss_r["mostRecentDateofLoss"] = nfip_loss_r['mostRecentDateofLoss'].dt.date

nfip_loss_r = nfip_loss_r.drop(columns=["state","county","communityName", "id", "censusBlockGroup",
                                      "asOfDate - Effective date on the File","fmaRl - Flood Metigation Assistance repetitive Loss",
                                      "fmaSrl - Flood Metigation Assistance Severe Repetitive Loss ",
                                      "originalConstructionDate"])

nfip_loss_r['mostRecentDateofLoss'] = pd.to_datetime(nfip_loss_r["mostRecentDateofLoss"]).dt.strftime('%Y-%m-%d')

nfip_loss_r.rename(columns={'nfipRl - Repetitive Loss of more than 1000 per claim':'RepeatLoss >1000',
                          'nfipSrl - Severe Repetitive Loss of cumulative claim payments of more than 10000':'CumulativePayments >10000',
                          'originalNBDate - Date of the Beginning of the Flood Policy':'PolicyStartDate'}, inplace=True)

# nfip_loss['zipCode'] = nfip_loss.apply(
#     lambda row: row['reportedCity'] if pd.notna(row['reportedCity']) and str(row['reportedCity'].isdigit()) 
#     else row['reportedCity'], axis=1  
# ) 

# nfip_loss['reportedCity'] = nfip_loss.apply(
#     lambda row: None if pd.notna(row['reportedCity']) and str(row['reportedCity'].isdigit())
#     else row['reportedCity'], axis=1
# )

#Summarizing missing and non-missing entries

summary = pd.DataFrame({
    'Total Entries' : nfip_loss_r.shape[0],
    'Non-NaN Count' : nfip_loss_r.notna().sum(),
    'NaN Count' : nfip_loss_r.isna().sum(),
    'Percentage Missing' : (nfip_loss_r.isna().sum() / nfip_loss_r.shape[0]) * 100
})

nfip_loss = nfip_loss_r.dropna(subset=['latitude', 'zipCode'], how='any')

summary = pd.DataFrame({
    'Total Entries' : nfip_loss.shape[0],
    'Non-NaN Count' : nfip_loss.notna().sum(),
    'NaN Count' : nfip_loss.isna().sum(),
    'Percentage Missing' : (nfip_loss.isna().sum() / nfip_loss.shape[0]) * 100
})

#nfip_loss['zipCode'] = nfip_loss['zipCode'].astype(int)

print(summary)

nfip_loss = nfip_loss.reset_index(drop=True)

census_r = pd.read_csv("data/censusdata.csv", index_col=0) 

census = census_r.iloc[2:]

nan_summary_census = census.isna().sum()

print(nan_summary_census)










#nfip_loss.fillna(0)

#nfip_loss.to_csv('data/nfip_loss_cld.csv', index=False)

#for index, row in nfip_loss.iterrows():
    #if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
       # folium.Marker(
        #location=[row['latitude'], row['longitude']],
        #popup=row['reportedCity']
        #).add_to(m)

#m.save("m.html")
#webbrowser.open("m.html")

#flds_by_year = duckdb.sql("SELECT YEAR(mostRecentDateofLoss) as year, COUNT(*) AS total_floods FROM nfip_loss GROUP BY YEAR(mostRecentDateofLoss) ORDER BY year; ")

#flds_by_zip = duckdb.sql("SELECT communityIdNumber, stateAbbreviation, COUNT(zipCode) as ZIPcnt, FROM nfip_loss GROUP BY stateAbbreviation, communityIdNumber ORDER BY communityIdNumber;")

#group_coord_querry = """SELECT list([latitude, longitude]) AS coordinate_groups 
                        #FROM nfip_loss 
                        #WHERE latitude IS NOT NULL AND longitude IS NOT NULL 
                        #GROUP BY communityIdNumber;"""

#grouped_com_coords = duckdb.execute(group_coord_querry).fetchall()

#m = folium.Map(location=[37.7749, -122.4194], zoom_start=5)

#for row in grouped_com_coords:
        #coords = row[0]
        #folium.Polygon(locations=coords, color="red", 
                       #weight=2.5, fill=True, fill_opacity=0.5).add_to(m)

#m.save("map.html")
#webbrowser.open("map.html")

#distinct_states_q = duckdb.sql("SELECT DISTINCT stateAbbreviation AS State, COUNT('nfipRl - Repetitive Loss of more than 1000 per claim') AS RepetitiveClaims FROM nfip_loss GROUP BY stateAbbreviation ORDER BY State;")

#distinct_states = duckdb.execute(distinct_states_q).fetchall()

#print(distinct_states_q)






