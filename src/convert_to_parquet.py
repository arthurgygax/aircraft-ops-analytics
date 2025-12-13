# src/convert_to_parquet.py
import pandas as pd
import tarfile
import gzip
from pathlib import Path

# --- Configuration ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
MASTER_PARQUET_FILE = PROCESSED_DATA_DIR / "master_flight_data.parquet"

def main():
    PROCESSED_DATA_DIR.mkdir(exist_ok=True)
    
    raw_files = list(RAW_DATA_DIR.glob("states_*.csv.tar"))
    if not raw_files:
        print(f"ERROR: No raw data files (*.csv.tar) found in {RAW_DATA_DIR}.")
        return

    print(f"Found {len(raw_files)} file(s) to process.")
    all_dataframes = []

    # RAW NAMES from the OpenSky CSV format
    col_names = [
        "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate", 
        "callsign", "on_ground", "alert", "spi", "squawk", "baroaltitude", 
        "geoaltitude", "lastposupdate", "lastcontact"
    ]
    
    # We keep these raw columns
    cols_to_use = [
        "time", "icao24", "lat", "lon", "velocity", "vertrate", 
        "callsign", "on_ground", "baroaltitude"
    ]

    for tar_path in raw_files:
        print(f"--> Processing {tar_path.name}...")
        with tarfile.open(tar_path, "r") as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.endswith('.csv.gz'):
                    csv_file_object = tar.extractfile(member)
                    with gzip.open(csv_file_object, mode='rt') as f:
                        df = pd.read_csv(
                            f,
                            header=None,       
                            names=col_names,   
                            usecols=cols_to_use,
                            low_memory=False, 
                            dtype=str          
                        )
                    all_dataframes.append(df)
                    break 
            
    if not all_dataframes:
        print("\nERROR: No data was loaded. Aborting.")
        return

    print("Combining all data...")
    master_df = pd.concat(all_dataframes, ignore_index=True)

    # Remove CSV header rows that might be stuck in the data
    master_df = master_df[master_df['time'] != 'time']

    print("Renaming columns to standard names...")
    # --- MAPPING TO RAW COLUMN NAMES ---
    master_df = master_df.rename(columns={
        'time': 'timestamp', 
        'baroaltitude': 'altitude',
        'velocity': 'groundspeed', 
        'vertrate': 'vertical_rate',
        'lat': 'latitude',
        'lon': 'longitude'
    })

    print("Converting data types...")
    numeric_cols = ['timestamp', 'longitude', 'latitude', 'altitude', 'groundspeed', 'vertical_rate']
    for col in numeric_cols:
        master_df[col] = pd.to_numeric(master_df[col], errors='coerce')
    
    master_df.dropna(subset=numeric_cols + ['icao24'], inplace=True)

    # --- AIRLINE PARSING LOGIC ---
    print("Extracting airlines...")
    master_df['callsign'] = master_df['callsign'].fillna('').str.strip()
    master_df['airline'] = master_df['callsign'].str[:3].str.upper()

    # Keep only valid 3-letter ICAO codes
    valid_airline_format = master_df['airline'].str.match(r'^[A-Z]{3}$')
    master_df = master_df[valid_airline_format.fillna(False)]

    print("Converting units (Metric -> Imperial)...")
    master_df['altitude'] = master_df['altitude'] * 3.28084
    master_df['vertical_rate'] = master_df['vertical_rate'] * 3.28084
    master_df['groundspeed'] = master_df['groundspeed'] * 1.94384

    master_df['timestamp'] = pd.to_datetime(master_df['timestamp'], unit='s', utc=True)
    master_df['on_ground'] = master_df['on_ground'].apply(lambda x: str(x).lower() == 'true')

    print(f"Saving optimized Parquet file to {MASTER_PARQUET_FILE}...")
    master_df.to_parquet(MASTER_PARQUET_FILE, engine='pyarrow')

    print("\nConversion complete!")
    print(f"Total clean rows: {len(master_df)}")

if __name__ == "__main__":
    main()