import pandas as pd
from sys import argv
import re
import os

# get folder path from command line argument, e.g. python combine_csvs output/pokemon.csv
folder_path = re.sub('\/$', '', argv[1])
# get output file by adding _combined to folder path
output_file_path = re.sub(r'\.(\w+)$', r'_combined.\1', folder_path)

# Get names of CSV files
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Read each file
dfs = []
for csv in csv_files:
    df= pd.read_csv(os.path.join(folder_path, csv), header=0)
    dfs.append(df)

if len(dfs) != 0:
    # combine into one dataframe
    final_df = pd.concat(dfs, ignore_index=True)
    
    # write to one file
    final_df.to_csv(output_file_path, index=False, mode="a", header=not os.path.exists(output_file_path))
    
    #delete each file
    for csv in csv_files:
        os.remove(os.path.join(folder_path, csv))
        os.remove(os.path.join(folder_path, "." + csv + ".crc"))