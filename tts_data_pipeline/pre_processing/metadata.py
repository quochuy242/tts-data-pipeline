import os
import json
import pandas as pd


FOLDER_PATH = '/Users/trantrieunghi/Downloads/metadata'
          

def convert_duration(time_str):
    """
    Convert a time string in the format "HH:MM:SS" to seconds.
    """
    try:
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except ValueError:
        return None


for file in os.listdir(FOLDER_PATH):
    if file.endswith('.json'):
        file_path = os.path.join(FOLDER_PATH, file)

        with open(file_path) as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Error decoding JSON in file {file_path}")
                continue

        if isinstance(data, dict):
            data = [data]

        df = pd.DataFrame(data)
        if 'duration' in df.columns:
            df['duration_in_seconds'] = df['duration'].apply(convert_duration)  
        
        output_path = os.path.join(FOLDER_PATH, f'{file.replace(".json", ".csv")}')
        df.to_csv(output_path, index=False)
