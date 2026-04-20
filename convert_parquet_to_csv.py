import pandas as pd
import os
import sys

def convert_parquet_to_csv(parquet_file, csv_file, sample_size=None):
    if not os.path.exists(parquet_file):
        print(f"Error: Could not find '{parquet_file}'")
        print("Please make sure the file is in the same folder as this script, or provide the full path.")
        return
    
    print(f"Reading '{parquet_file}'...")
    
    try:
        # Read the parquet file
        df = pd.read_parquet(parquet_file)
        
        # If a sample size is specified, take a random sample
        # This is highly recommended for designing dashboards to keep things fast
        if sample_size and len(df) > sample_size:
            print(f"Sampling {sample_size} rows out of {len(df)} total rows...")
            df = df.sample(n=sample_size, random_state=42)
        else:
            print(f"Processing all {len(df)} rows...")
            
        print(f"Writing to '{csv_file}'...")
        # Write to CSV
        df.to_csv(csv_file, index=False)
        print(f"Successfully converted! CSV saved as '{csv_file}'")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Tip: You might need to install 'fastparquet' or 'pyarrow'. Run: pip install pandas pyarrow")

if __name__ == "__main__":
    # ---------------------------------------------------------
    # USER CONFIGURATION: Change these variables as needed
    # ---------------------------------------------------------
    
    # Replace with the exact name of your parquet file
    INPUT_FILE = r"C:\Users\Abdullah\Downloads\nyc_taxi_clean.parquet" 
    
    # The name of the CSV file that will be created
    OUTPUT_FILE = "nyc_data_sample.csv"
    
    # Set to an integer to take a smaller sample (recommended for designing)
    # Set to None to convert the entire file (could be huge and slow!)
    SAMPLE_ROWS = 50000 
    
    convert_parquet_to_csv(INPUT_FILE, OUTPUT_FILE, SAMPLE_ROWS)
