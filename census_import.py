import pandas as pd 
import glob
import os

csv_files = glob.glob("data/censusdata/20*.csv")

dataframes = []

for file in csv_files:
   file_name = os.path.basename(file)
   year = int(file_name[:4])
   df = pd.read_csv(file, usecols=[0,1,2], names=["Geography","Geographic Area Name", "Total"], low_memory=False) 
   df['YEAR'] = year
   dataframes.append(df)



combined_df = pd.concat(dataframes, ignore_index=True)

combined_df.to_csv("data/combined_census.csv", index=False)
