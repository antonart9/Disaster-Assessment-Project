import folium
from folium.plugins import MarkerCluster
import pandas as pd
import numpy as np
import webbrowser
import duckdb
import gdown 
import os

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

#print(summary) -- part of the project testing

nfip_loss = nfip_loss.copy()

nfip_loss['PolicyStartDate'] = pd.to_datetime(nfip_loss['PolicyStartDate'])

nfip_loss['PolicyStartYear'] = nfip_loss['PolicyStartDate'].dt.year

nfip_loss = nfip_loss[(nfip_loss['PolicyStartYear'] >= 2010) & (nfip_loss['PolicyStartYear'] <=2022)]

nfip_loss = nfip_loss.reset_index(drop=True)

#print(nfip_loss.head()) -- part of the project testing

census_r = pd.read_csv("data/censusdata.csv", index_col=0) 

census = census_r.iloc[2:]

census = census.copy()

census['zipCode'] = census['Geographic Area Name'].str[-5:]

#print(census.head()) -- part of the project testing

# Grouping claims by zip code and Policy start year and aggreaging total claims into total losses
# and using the first coordinate set of the grouped data.

nfip_t_claims = nfip_loss.groupby(['zipCode', 'PolicyStartYear','stateAbbreviation'], as_index=False).agg({
    'totalLosses' : 'sum',
    'latitude' : 'first',
    'longitude' : 'first'
})

nfip_t_claims['latitude'] = nfip_t_claims['latitude'].fillna(0)

nfip_t_claims['longitude'] = nfip_t_claims['longitude'].fillna(0)

nfip_t_claims.rename(columns={'totalLosses' : 'totalClaims'}, inplace=True)

nfip_t_claims['zipCode'] = (nfip_t_claims['zipCode'].astype(int).astype(str).str.zfill(5))

nfip_t_claims['PolicyStartYear'] = nfip_t_claims['PolicyStartYear'].astype(int).astype(str)

# print(nfip_t_claims.head()) -- part of the project testing

# Combinning Census Data with Historical Flood Claims Data using DuckDB

census_merge = duckdb.query("""

SELECT nfip_t_claims.PolicyStartYear,
       nfip_t_claims.zipCode,
       nfip_t_claims.stateAbbreviation as State,                     
       census.Total AS TotalDwellings,
       nfip_t_claims.latitude,
       nfip_t_claims.longitude,           
       nfip_t_claims.totalClaims as TotalFloodClaims
FROM nfip_t_claims
LEFT JOIN census 
ON nfip_t_claims.zipCode = census.zipCode AND nfip_t_claims.PolicyStartYear = census.YEAR  

""").to_df()

census_merge['TotalFloodClaims'] = pd.to_numeric(census_merge['TotalFloodClaims'], errors='coerce')

census_merge['TotalDwellings'] = pd.to_numeric(census_merge['TotalDwellings'], errors='coerce')

census_merge['FloodRisk'] = ((census_merge['TotalFloodClaims'] / census_merge['TotalDwellings'])*100).round(2).astype(str) + '%'


#print(census_merge.head()) -- part of the project testing

results_folder = "results"
output_file = os.path.join(results_folder, "flood_risk_data.csv")

os.makedirs(results_folder, exist_ok=True)

census_merge.to_csv(output_file, index=False)

# print(census_merge.isna().sum()) -- part of the project testing

# Generating an interactive Folium Map with Risk Analysis Data

m = folium.Map(location=[37.7749, -95.7129], zoom_start=5)

cluster = MarkerCluster().add_to(m)

for _, row in census_merge.iterrows():
    folium.Marker (
        location=[row['latitude'], row['longitude']],
        popup=f"ZIP: {row['zipCode']}<br>Flood Risk: {row['FloodRisk']}",
    ).add_to(cluster)

m.save("results/flood_Risk_m.html")

print("The Flood Risk Analysis csv File, and the Flood Risk Map Generated Successfully in the Results Folder")












