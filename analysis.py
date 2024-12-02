import re
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

# Function to read and process the patterns file
def process_patterns_file(file_path):
    patterns = []
    with open(file_path, 'r') as f:
        for line in f:
            match = re.search(r'#SUP: (\d+)', line)
            if match:
                support = int(match.group(1))
                indices = [int(idx) for idx in line.split('#SUP:')[0].split()]
                patterns.append((indices, support))
    return patterns

# Function to rename columns based on provided mappings
def rename_columns(matrix_data_df):      
    country_fips_df = pd.read_csv('country_FIPS.csv', sep='\t')
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
    return new_columns


# Function to process data for a specific year
def process_year(year):
    base_path = f'{year}'
    days = sorted(os.listdir(base_path))

    daily_patterns = []  # Patterns for each day
    daily_index_to_item = []  # Index-to-item mappings for each day
    total_transactions_list = []  # Total transactions for each day

    for day_index, day in enumerate(tqdm(days, desc=f'Processing {year}')):
        day_path = os.path.join(base_path, day)
        csv_file = os.path.join(day_path, f'{day}.csv')
        patterns_file = os.path.join(day_path, 'patterns.txt')

        # Load CSV file to get total number of transactions
        df = pd.read_csv(csv_file)
        total_transactions = len(df)
        total_transactions_list.append(total_transactions)

        # Read the matrix_data DataFrame to get the original column names
        matrix_data_df = pd.read_csv(csv_file)

        # Rename columns to get the real theme names
        new_columns = rename_columns(matrix_data_df)
        index_to_item = {idx: name for idx, name in enumerate(new_columns)}
        daily_index_to_item.append(index_to_item)

        # Process patterns file
        patterns = process_patterns_file(patterns_file)

        # Compute percentage supports and collect patterns
        day_patterns = {}
        for indices, support in patterns:
            pattern_key = tuple(sorted(indices))
            percentage_support = support / total_transactions * 100
            day_patterns[pattern_key] = percentage_support

        daily_patterns.append(day_patterns)

    # Collect all patterns across all days
    all_patterns = set()
    for day_patterns in daily_patterns:
        all_patterns.update(day_patterns.keys())

    num_days = len(days)
    # Initialize pattern supports over time
    pattern_supports_over_time = {pattern: [np.nan]*num_days for pattern in all_patterns}

    # Fill in the supports
    for day_index, day_patterns in enumerate(daily_patterns):
        for pattern, support in day_patterns.items():
            pattern_supports_over_time[pattern][day_index] = support

    return (pattern_supports_over_time, days, daily_index_to_item, total_transactions_list)

# Function to map pattern indices to actual items for a given day
def pattern_to_items(pattern, index_to_item):
    return [index_to_item.get(idx, f'Item{idx}') for idx in pattern]


# Process data for each year, give the folder names from process.py
years = ['2021', '2022', '2023'] 
results = {}
color_mapping = {}

for year in years:
    (pattern_supports_over_time, days, daily_index_to_item, total_transactions_list) = process_year(year)
    print(daily_index_to_item)