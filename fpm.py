import pandas as pd
import os
from spmf import Spmf



def rename_columns(matrix_data_df):      
    country_fips_df = pd.read_csv('country_FIPS.csv', sep='\t')
    country_columns = [col for col in matrix_data_df.columns if col.startswith('Country_')]
    country_mapping = dict(zip(country_fips_df['FIPS'], country_fips_df['ADMIN']))
    wb_sdg_df = pd.read_csv('world_bank_to_sdg.csv', sep='\t')
    wb_mapping = dict(zip(wb_sdg_df['WB_NUM'].astype(str), wb_sdg_df['WB_NAME']))

    new_columns = []
    country_num = 0
    theme_num = 0
    for col in matrix_data_df.columns:
        if col.startswith('Country_'):
            country_code = col.split('_')[1]
            full_name = country_mapping.get(country_code, country_code)
            new_columns.append(full_name)
            country_num += 1
        elif col.startswith('Theme_WB'):
            wb_num = ''.join(filter(str.isdigit, col))
            full_name = wb_mapping.get(wb_num, col)  
            new_columns.append(full_name)
            theme_num += 1
        else:
            new_columns.append(col)
    return new_columns


folder = "2023" # Which folder of data to use, download.py must be run first to download the data

for dir in os.listdir(folder):
    
    matrix_data_df = pd.read_csv(f'{folder}//{dir}//{dir}.csv', sep='\t')
    country_fips_df = pd.read_csv('country_FIPS.csv', sep='\t')
    country_columns = [col for col in matrix_data_df.columns if col.startswith('Country_')]
    country_mapping = dict(zip(country_fips_df['FIPS'], country_fips_df['ADMIN']))
    wb_sdg_df = pd.read_csv('world_bank_to_sdg.csv', sep='\t')
    wb_mapping = dict(zip(wb_sdg_df['WB_NUM'].astype(str), wb_sdg_df['WB_NAME']))

    new_columns = []
    for col in matrix_data_df.columns:
        if col.startswith('Country_'):
            country_code = col.split('_')[1]
            full_name = country_mapping.get(country_code, country_code)
            new_columns.append(full_name)
        elif col.startswith('Theme_WB'):
            wb_num = ''.join(filter(str.isdigit, col))
            full_name = wb_mapping.get(wb_num, col)  
            new_columns.append(full_name)
        else:
            new_columns.append(col)
    matrix_data_df.columns = new_columns
    
    

    spmf = Spmf("FPMax", input_filename=f"{folder}/{dir}/{dir}_t.txt", output_filename=f"{folder}/{dir}/patterns.txt", arguments=[0.01])
    spmf.run()

