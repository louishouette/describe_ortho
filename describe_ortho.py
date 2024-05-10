import os
import json
import subprocess
import pandas as pd
import rasterio
from tqdm import tqdm

def get_exif_metadata(image_path):
    """Uses ExifTool to extract specified metadata from the image file."""
    command = ['exiftool', '-BandName', '-AcquisitionDuration', '-RadiometricCorrection', '-PixelScale', '-GDALNoData', '-j', image_path]
    result = subprocess.run(command, capture_output=True, text=True)
    metadata = json.loads(result.stdout)[0]  # Assumes ExifTool outputs valid JSON

    return {
        'band_name': metadata.get('BandName', None),
        'acquisition_duration_in_minutes': metadata['AcquisitionDuration'] / 60 if 'AcquisitionDuration' in metadata else None,
        'radiometric_correction': metadata.get('RadiometricCorrection', None),
        'gsd': float(metadata['PixelScale'].split()[0]) * 100 if 'PixelScale' in metadata else None,
        'gdal_no_data': metadata.get('GDALNoData', None)
    }

def process_csv_metadata(csv_path):
    """Processes metadata from a CSV file and aggregates necessary information."""
    cols = ['GPS Altitude', 'Relative Altitude', 'Camera Model Name', 'Drone Model', 'Create Date', 'UTC At Exposure', 'Gps Status']
    try:
        # Read only the existing columns from the CSV
        available_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
        relevant_cols = [col for col in cols if col in available_cols]

        if not relevant_cols:
            raise ValueError("None of the specified columns are available in the CSV file.")

        metadata = pd.read_csv(csv_path, usecols=relevant_cols)

        # Check and process each column if it exists
        if 'Relative Altitude' in metadata:
            metadata['Relative Altitude'] = metadata['Relative Altitude'].astype(str).str.lstrip('+').astype(float)

        if 'Create Date' in metadata:
            metadata['Create Date'] = pd.to_datetime(metadata['Create Date'].str.replace('(\d{4}):(\d{2}):(\d{2})', r'\1-\2-\3', regex=True), errors='coerce')

        if 'UTC At Exposure' in metadata:
            metadata['UTC At Exposure'] = pd.to_datetime(metadata['UTC At Exposure'].str.replace('(\d{4}):(\d{2}):(\d{2})', r'\1-\2-\3', regex=True).str.replace(',', '.', regex=False), errors='coerce')

        return {
            'gps_altitude': metadata['GPS Altitude'].median() if 'GPS Altitude' in metadata else None,
            'relative_altitude': metadata['Relative Altitude'].median() if 'Relative Altitude' in metadata else None,
            'camera_model_name': metadata['Camera Model Name'].mode().iloc[0] if 'Camera Model Name' in metadata and not metadata['Camera Model Name'].mode().empty else None,
            'drone_model': metadata['Drone Model'].mode().iloc[0] if 'Drone Model' in metadata and not metadata['Drone Model'].mode().empty else None,
            'create_date': metadata['Create Date'].min().isoformat() if 'Create Date' in metadata and metadata['Create Date'].min() is not pd.NaT else None,
            'utc_at_exposure': metadata['UTC At Exposure'].min().isoformat() if 'UTC At Exposure' in metadata and metadata['UTC At Exposure'].min() is not pd.NaT else None,
            'gps_status': metadata['Gps Status'].mode().iloc[0] if 'Gps Status' in metadata and not metadata['Gps Status'].mode().empty else None
        }
    except ValueError as e:
        print(f"Error processing CSV file {csv_path}: {e}")
        return {}

def get_bbox(image_path):
    """Calculate the bounding box of the raster and return it as a dictionary using rasterio."""
    with rasterio.open(image_path) as dataset:
        bounds = dataset.bounds
        return {
            'bbox': {
                'xmin': bounds.left,
                'ymin': bounds.bottom,
                'xmax': bounds.right,
                'ymax': bounds.top
            }
        }

def save_metadata_to_file(metadata, directory, filename):
    """Saves processed metadata to a JSON file."""
    file_path = os.path.join(directory, f"{filename}.json")
    with open(file_path, 'w') as f:
        json.dump(metadata, f, indent=4)

def main(directory):
    """Main function to process each orthophoto's metadata, calculate bounding box, and save it as a JSON file."""
    for root, dirs, files in os.walk(directory):
        if 'ortho' in dirs:
            ortho_dir = os.path.join(root, 'ortho')
            tif_files = [file for file in os.listdir(ortho_dir) if file.endswith('.tif') and not file.startswith('.')]

            for file in tqdm(tif_files, desc="Processing Orthophotos"):
                image_path = os.path.join(ortho_dir, file)
                csv_path = os.path.join(root, 'metadata.csv')
                if os.path.exists(csv_path):
                    metadata = process_csv_metadata(csv_path)
                    if metadata:
                        try:
                            exif = get_exif_metadata(image_path)
                            bbox = get_bbox(image_path)
                            metadata.update(exif)
                            metadata.update(bbox)
                            save_metadata_to_file(metadata, ortho_dir, os.path.splitext(file)[0])
                        except rasterio.errors.RasterioIOError as e:
                            print(f"Error reading {file}: {e}")
                else:
                    print(f"No metadata.csv found in {root}. Skipping file {file}.")

if __name__ == "__main__":
    main('/Volumes/ExDisk3-4To/Flight folders/Pix4D')
