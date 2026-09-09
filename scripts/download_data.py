"""
NASA CMAPSS Dataset Downloader
Fetches the official benchmark dataset for Predictive Maintenance.
"""

import os
import zipfile
import requests
from pathlib import Path

CMAPSS_ZIP_URL = "https://raw.githubusercontent.com/mohamedelshabani/CMAPSS-NASA-Turbofan-Jet-Engine-Degradation-Simulation/master/CMAPSSData.zip"

def download_and_extract_data(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / "CMAPSSData.zip"
    
    # Check if files already exist
    expected_file = output_dir / "train_FD001.txt"
    if expected_file.exists():
        print(f"Data already present in {output_dir}")
        return
        
    print(f"Downloading CMAPSS dataset from {CMAPSS_ZIP_URL}...")
    response = requests.get(CMAPSS_ZIP_URL, stream=True)
    response.raise_for_status()
    
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    print("Extracting ZIP archive...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)
        
    print(f"Extracted files to {output_dir}")
    # Clean up zip file
    if zip_path.exists():
        os.remove(zip_path)

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    data_raw_dir = base_dir / "data" / "raw"
    download_and_extract_data(data_raw_dir)
