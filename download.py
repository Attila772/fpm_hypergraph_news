from datetime import datetime
from datetime import timedelta
import requests
import zipfile
import pandas as pd
import os
import shutil
import re
import dask.dataframe as dd
from dask import optimize
from tqdm import tqdm
import numpy as np
from spmf import Spmf
import hypernetx as hnx
import matplotlib.pyplot as plt
import networkx as nx
import os


tqdm.pandas()
themes_df = pd.read_csv('world_bank_to_sdg.csv', sep='\t')
sdg_13_themes_df = themes_df[themes_df['SDG'] == 13]
unique_themes = sdg_13_themes_df['WB_SHORT'].unique().tolist()
themes = unique_themes
themes_pattern = '|'.join(themes)

def extract_country_code(location_string):
    match = re.search(r'^(?:[^#]*#){2}([^#]*)', location_string)
    if match:
        return match.group(1)
    else:
        return None
    
def extract_tone(v2tone):
    if pd.isna(v2tone):
        return np.nan
    try:
        return float(v2tone.split(',')[0])
    except :
        return None # or some other default value

                 
def extract_country_code(location_string):
    match = re.search(r'^(?:[^#]*#){2}([^#]*)', location_string)
    if match:
        return match.group(1)
    else:
        return None

def split_and_expand(partition_df, column_name, max_columns):
    expanded = partition_df[column_name].str.split(';', expand=True)
    # Ensure the DataFrame has max_columns columns
    for i in range(expanded.shape[1], max_columns):
        expanded[i] = np.nan
    # Name columns for a cleaner melt later
    expanded.columns = [f"{column_name}_{i}" for i in range(max_columns)]
    return expanded

def get_time_frame(start_date_str, end_date_str):
    # Parsing the input date strings
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Setting the time to midnight for both dates
    start_time = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    # Adding one day to the end date and setting the time to midnight
    end_time = (end_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    return start_time, end_time


folder_path = "data"
today_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
if os.path.exists(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        # Extract the date from the filename
        file_date_str = filename.split('.')[0]  # Assuming the date is at the beginning of the filename
        print(file_date_str, filename, today_str)
        # Check if the date in the filename is not today
        if not file_date_str.startswith(today_str):
            # Check if it's a file or directory
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Remove file or symbolic link
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Remove directory
else:
    print(f"'{folder_path}' does not exist.")
print(f"Old files in '{folder_path}' have been removed!")




# Define column names
column_names = ['GKGRECORDID', 'V21DATE', 'V2SOURCECOLLECTIONIDENTIFIER', 'V2SOURCECOMMONNAME',
                'V2DOCUMENTIDENTIFIER',  'V1COUNTS', 'V21COUNTS', 'V1THEMES',
                'V2ENHANCEDTHEMES', 'V1LOCATIONS', 'V2ENHANCEDLOCATIONS', 'V1PERSONS', 'V2ENHANCEDPERSONS',
                'V1ORGANIZATIONS', 'V2ENHANCEDORGANIZATIONS', 'V15TONE', 'V21ENHANCEDDATES', 'V2GCAM',
                'V21SHARINGIMAGE', 'V21RELATEDIMAGES', 'V21SOCIALIMAGEEMBEDS', 'V21SOCIALVIDEOEMBEDS',
                'V21QUOTATIONS', 'V21ALLNAMES', 'V21AMOUNTS', 'Inserted', "filename"]



start_time, end_time = get_time_frame('2023-10-30', '2024-01-13')
print(start_time, end_time)
dfs = []

downloaded_files = set(os.listdir('data'))
expected_columns = len(column_names)
# Loop through each 15-minute interval within the specified duration
current_time = start_time
while current_time < end_time:
    starting_current_time = current_time
    day_end_time = (current_time + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    while current_time < day_end_time:
        
        date = current_time.strftime('%Y%m%d%H%M%S')
        zipname = date + '.gkg.csv.zip'
        csvname = date + '.gkg.csv'
        
        if csvname in downloaded_files:
            print(f'Skipping {zipname}, already downloaded')
           
            current_time += timedelta(minutes=15)
            try:
                df = dd.read_csv(f'data/{csvname}', sep='\t', header=None, assume_missing=True, encoding = "ISO-8859-1",
                                dtype={
                                        19: 'object',
                                        20: 'object',
                                        5: 'object',
                                        6: 'object',
                                        22: 'object',
                                        21: 'object',
                                        16: 'object',
                                    }
                                ,usecols=range(expected_columns))
                df.columns = column_names
               
                df = df[df['V1LOCATIONS'].notnull() & df['V1THEMES'].notnull() & df['V1THEMES'].str.contains(themes_pattern)]  
                
                if 'df_combined' in locals():
                
                    df_combined = dd.concat([df_combined, df])
                else:
                   
                    df_combined = df
            except Exception as e:
                print('Error is:', e)
            continue


        # Send a GET request to the URL and download the file
        url = f'http://data.gdeltproject.org/gdeltv2/{zipname}'
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            # Create the "data" folder if it doesn't exist
            os.makedirs('data', exist_ok=True)

            # Save the downloaded file to the "data" folder
            with open(f'data/{zipname}', 'wb') as file:
                file.write(response.content)
                print(f'Downloaded {zipname}')

            # Extract the contents of the zip file
            with zipfile.ZipFile(f'data/{zipname}', 'r') as zip_ref:
                zip_ref.extractall('data')
            os.remove(f'data/{zipname}')
            # Add the downloaded file names to the set
            downloaded_files.add(zipname)
            downloaded_files.add(csvname)

            # Read the CSV file into a DataFrame
            try:
                df = dd.read_csv(f'data/{csvname}', sep='\t', header=None, assume_missing=True, encoding = "ISO-8859-1",
                                dtype={
                                        19: 'object',
                                        20: 'object',
                                        5: 'object',
                                        6: 'object',
                                        16: 'object',
                                        22: 'object',
                                        21: 'object',
                                    } 
                                ,usecols=range(expected_columns))
                

                df.columns = column_names
                
                df = df[df['V1LOCATIONS'].notnull() & df['V1THEMES'].notnull() & df['V1THEMES'].str.contains(themes_pattern)]  
                
                # Append the DataFrame to the list
                if 'df_combined' in locals():
                    df_combined = dd.concat([df_combined, df])
                    
                else:
                    df_combined = df
            except Exception as e:
                print(e)
                print(f'Failed to read {csvname}')
        else:
            print(f'Failed to download {zipname}')

        # Increment the start time by 15 minutes
        current_time += timedelta(minutes=15)
    print(f'Finished processing {starting_current_time} to {current_time}')
    result = df_combined.compute()
    result.columns = column_names
    formatted_time = starting_current_time.strftime('%Y%m%d')
    result.to_csv(f'temp_data//{formatted_time}.csv', sep='\t', encoding='utf-8', index=False)
    del df_combined
    try:
        print("Removing data folder")
        shutil.rmtree("data")
        os.makedirs('data', exist_ok=True)
    except:
        pass
    
    