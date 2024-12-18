## US Flood Data Assessment Project for 2010-2022

This project organizes FEMA National Flood Insurance Program Claims data and compares it to the US Census Bureau report for total Dwellings per Zip code. Using an aggregated number of claims submitted per zip code and comparing it to the aggregated number of dwellings gives a rough risk estimate of a dwelling submitting a flood claim of more than 1000 in damages (the flood data does not contain any claims that were for an amount less than that). 

## Prerequisites:
- Latest Version of Python
- Dependencies listed in requirements.txt

## Data:

The project uses two main data CSV files, one from FEMA and a combined file of 12 years’ worth of data from the Census Bureau. There was a program written (census_import.py) to quickly combine 12 files into one which was used for the remainder of the project. 

OpenFEMA Dataset: NFIP Multiple Loss Properties https://www.fema.gov/openfema-data-page/nfip-multiple-loss-properties-v1

        Data Fields Used:

            stateAbbreviation
            zipCode
            latitude
            longitude
            originalNBDate
            totalLosses

Census Data: Report Generated using data.census.gov 
        
        Data Fields Used:
            Housing Units
            All 5-digit Zip Code 
            Year (filtered)

## Results:

Results of the project will be generated in the results folder of the directory, where two files should generate, flood_risk_data.csv and flood_Risk_m.html. The HTML file is an interactive Folium map that shows markers of risk percentages per zip code. Another representation of the data was created using  a Tableau dashboard that shows weighted average and average flood risk for the last 5 years per State. Conclusions may not be surprising, as the South, and Southeast coastal states have a higher risk of submitting a flood claim, but Missouri had the largest average flood claim risk. Arizona had the least average flood claim risk. There was no flood data for Wyoming. 

## Instructions.
1. Clone the Repository from GitHub and load the folder into the code intepreter. 

2. Create a virtual environment to run main.py file:
        
        Virtual Environment Commands
        | Command | Linux/Mac | GitBash | PowerShell |
        | ------- | --------- | ------- | ---------- |
        | Create | `python3 -m venv venv` | `python -m venv venv` | `python -m venv venv` |
        | Activate | `source venv/bin/activate` | `source venv/Scripts/activate` | `./venv/Scripts/activate` |
        | Install | `pip install -r requirements.txt` | `pip install -r requirements.txt` |`pip install -r requirements.txt` |
        | Deactivate | `deactivate` | `deactivate` | `deactivate` |

3. Ensure modules are installed and Python is updated to the latest version.

4. Open main.py in the project directory and run the file while in the virtual environment.

5. Once a Success Message has been generated in the terminal, refer to the result folder for the Flood Risk map html file, and review my Tableau dashboard (https://public.tableau.com/views/FloodRiskAnalysisProjectBar/Sheet1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link) for an easy to read bar graph. 

## Features:
1. Read in two csv files.
2. Perform Data Cleaning.
3. Performed a Join using DuckDB
4. Generated an interactive map using Folium.
5. Generated an easy to read bar graph using Tableau. 
6. Utilization of Virtual Environment. 

## Proccess:
Data from FEMA's website was insufficient for concise analysis on its own. To calculate flood claim risk, I compared the number of claims per zip code to the total number of dwellings in the same zip code, using Census Bureau data. Risk percentages were calculated and visualized on an interactive map using Folium. Results were further explored in Tableau to identify regional trends. This project is not intended as a definitive flood risk estimate but rather a demonstration of data analysis techniques involving maps and visualization.