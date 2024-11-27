import folium
import pandas as pd
import numpy as np
import webbrowser
import duckdb
import gdown 

url  ="https://drive.google.com/file/d/1_2aV0idVYIhufdc-V9A7tZ_xhrqy4OcB/view?usp=drive_link"
output = "data/multipleLossProperties2.csv"
gdown.download(url, output, fuzzy=True)


nfip_loss_r = pd.read_csv("data/multipleLossProperties2.csv", index_col=0) 

nfip_loss_r['asOfDate - Effective date on the File'] = pd.to_datetime(nfip_loss_r['asOfDate - Effective date on the File'])
nfip_loss_r["asOfDate - Effective date on the File"] = nfip_loss_r['asOfDate - Effective date on the File'].dt.date


nfip_loss_r['originalConstructionDate'] = pd.to_datetime(nfip_loss_r['originalConstructionDate'])
nfip_loss_r["originalConstructionDate"] = nfip_loss_r['originalConstructionDate'].dt.date

nfip_loss_r['originalNBDate - Date of the Beginning of the Flood Policy'] = pd.to_datetime(nfip_loss_r['originalNBDate - Date of the Beginning of the Flood Policy'])
nfip_loss_r["originalNBDate - Date of the Beginning of the Flood Policy"] = nfip_loss_r['originalNBDate - Date of the Beginning of the Flood Policy'].dt.date


nfip_loss_r['mostRecentDateofLoss'] = pd.to_datetime(nfip_loss_r['mostRecentDateofLoss'])
nfip_loss_r["mostRecentDateofLoss"] = nfip_loss_r['mostRecentDateofLoss'].dt.date

nfip_loss = nfip_loss_r.drop(columns=["state", "id", "censusBlockGroup"])
print(nfip_loss.head())

nfip_loss.fillna(0)

nfip_loss.to_csv('data/nfip_loss_cld.csv', index=False)


m = folium.Map(location=[44, -102], zoom_start=5)

#for index, row in nfip_loss.iterrows():
    #if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
       # folium.Marker(
        #location=[row['latitude'], row['longitude']],
        #popup=row['reportedCity']
        #).add_to(m)

m.save("m.html")
webbrowser.open("m.html")







