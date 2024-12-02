import os
import pandas as pd
import numpy as np
import re
from tqdm import tqdm

# Helper functions
def extract_tone(v2tone):
    if pd.isna(v2tone):
        return np.nan
    try:
        return float(v2tone.split(',')[0])
    except:
        return None

def extract_country_code(location_string):
    match = re.search(r'^(?:[^#]*#){2}([^#]*)', location_string)
    if match:
        return match.group(1)
    else:
        return None

tqdm.pandas()

# Load themes
themes_df = pd.read_csv('world_bank_to_sdg.csv', sep='\t')
sdg_13_themes_df = themes_df[themes_df['SDG'] == 13]
unique_themes = sdg_13_themes_df['WB_SHORT'].unique().tolist()
themes = unique_themes
themes_pattern = '|'.join(themes)

# Path to the "temp_data" folder
temp_data_folder = "temp_data" #the folder used by download.py to store the data
wrangled_folder = "2023" # This folder will contain the data needed for frequent pattern mining

# Ensure the "temp_data" folder exists
if not os.path.exists(temp_data_folder):
    print(f"The folder {temp_data_folder} does not exist.")
else:
    # Iterate over all CSV files in the "temp_data" folder
    for csv_file in os.listdir(temp_data_folder):
        if csv_file.endswith(".csv"):
            csv_path = os.path.join(temp_data_folder, csv_file)

            result = pd.read_csv(csv_path, low_memory=False, sep='\t')
            
            df_filtered = result[['GKGRECORDID', 'V1LOCATIONS', 'V1THEMES', 'V15TONE']]
            df_filtered['Locations_list'] = df_filtered['V1LOCATIONS'].str.split(';')
            df_filtered['Country_codes'] = df_filtered['Locations_list'].apply(lambda x: [extract_country_code(loc) for loc in x] if x is not None else None)
            df_filtered['Unique_Country_codes'] = df_filtered['Country_codes'].apply(lambda x: list(set(x)) if x is not None else None)
            df_filtered['Themes_list'] = df_filtered['V1THEMES'].str.split(';')
            df_filtered['General_Tone'] = df_filtered['V15TONE'].apply(extract_tone)

            print(df_filtered.head(10))

            def keep_selected_themes(themes_list, selected_themes):
                return [theme for theme in themes_list if any(sel_theme in theme for sel_theme in selected_themes)]

            df_filtered['Filtered_Themes_list'] = df_filtered['Themes_list'].apply(lambda x: keep_selected_themes(x, themes))
            df_filtered = df_filtered[df_filtered['Filtered_Themes_list'].apply(len) > 0]

            df_only_new = df_filtered[['GKGRECORDID', 'Filtered_Themes_list', 'General_Tone']]
            print(df_only_new.head(10))

            # Create new directory if "2021_wrangled" exists
            if os.path.exists(wrangled_folder):
                new_folder_name = os.path.splitext(csv_file)[0]
                new_folder_path = os.path.join(wrangled_folder, new_folder_name)
                os.makedirs(new_folder_path, exist_ok=True)

                csv_output_path = os.path.join(new_folder_path, f"{new_folder_name}.csv")
                transactions_output_path = os.path.join(new_folder_path, f"{new_folder_name}_t.txt")


                matrix_data_df = pd.DataFrame(index=df_filtered.index)
                
                for theme in tqdm(themes):
                    matrix_data_df[f'Theme_{theme}'] = df_filtered['Filtered_Themes_list'].apply(
                        lambda x: 1 if any(theme in t for t in x) else 0
                    )
                matrix_data_df.to_csv(csv_output_path, index=False)

                with open(transactions_output_path, 'w') as file:
                    for index, row in matrix_data_df.iterrows():
                        items = [str(i) for i, value in enumerate(row) if value == 1]
                        transaction_str = ' '.join(items)
                        file.write(f"{transaction_str}\n")
            else:
                print(f"The folder {wrangled_folder} does not exist. Files will not be saved.")
