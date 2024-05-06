import os
import json
import subprocess
import pandas as pd
from osgeo import gdal

def get_exiftool_metadata(image_path):
    """Uses ExifTool to extract specified metadata from the image file."""
    command = ['exiftool', '-BandName', '-AcquisitionDuration', '-RadiometricCorrection', '-PixelScale', '-GDALNoData', '-j', image_path]
    result = subprocess.run(command, capture_output=True, text=True)
    metadata = json.loads(result.stdout)[0]  # Assumes ExifTool outputs valid JSON
    return {
        'band_name': metadata.get('BandName', None),
        'acquisition_duration': metadata.get('AcquisitionDuration', None),
        'radiometric_correction': metadata.get('RadiometricCorrection', None),
        'pixel_scale': metadata.get('PixelScale', None),
        'gdal_no_data': metadata.get('GDALNoData', None)
    }

def process_metadata(csv_path):
    """Processes metadata from a CSV file and aggregates necessary information."""
    cols = ['GPS Altitude', 'Relative Altitude', 'Camera Model Name', 'Drone Model', 'Create Date', 'UTC At Exposure', 'Gps Status']
    metadata = pd.read_csv(csv_path, usecols=cols)

    processed_data = {}
    processed_data['gps_altitude'] = metadata['GPS Altitude'].median()
    processed_data['relative_altitude'] = metadata['Relative Altitude'].str.lstrip('+').astype(float).median()
    processed_data['camera_model_name'] = metadata['Camera Model Name'].mode().iloc[0] if not metadata['Camera Model Name'].mode().empty else None
    processed_data['drone_model'] = metadata['Drone Model'].mode().iloc[0] if not metadata['Drone Model'].mode().empty else None
    metadata['Create Date'] = pd.to_datetime(metadata['Create Date'], errors='coerce')
    metadata['UTC At Exposure'] = pd.to_datetime(metadata['UTC At Exposure'], errors='coerce')
    processed_data['create_date'] = metadata['Create Date'].min().isoformat() if not pd.isnull(metadata['Create Date'].min()) else None
    processed_data['utc_at_exposure'] = metadata['UTC At Exposure'].min().isoformat() if not pd.isnull(metadata['UTC At Exposure'].min()) else None
    processed_data['gps_status'] = metadata['Gps Status'].mode().iloc[0] if not metadata['Gps Status'].mode().empty else None

    return processed_data

def get_raster_bbox(image_path):
    """Calculate the bounding box of the raster."""
    dataset = gdal.Open(image_path)
    geotransform = dataset.GetGeoTransform()
    width = dataset.RasterXSize
    height = dataset.RasterYSize

    xmin = geotransform[0]
    ymax = geotransform[3]
    xmax = xmin + geotransform[1] * width
    ymin = ymax + geotransform[5] * height  # geotransform[5] is negative

    return xmin, ymin, xmax, ymax

def save_metadata_to_file(metadata, directory, filename, bbox):
    """Saves processed metadata to a JSON file including raster bounding box."""
    metadata['bbox'] = {'xmin': bbox[0], 'ymin': bbox[1], 'xmax': bbox[2], 'ymax': bbox[3]}
    file_path = os.path.join(directory, f"{filename}.json")
    with open(file_path, 'w') as f:
        json.dump(metadata, f, indent=4)

def main(directory):
    """Main function to process each orthophoto's metadata, calculate bounding box, and save it as a JSON file."""
    for root, dirs, files in os.walk(directory):
        if 'ortho' in dirs:
            ortho_dir = os.path.join(root, 'ortho')
            for file in os.listdir(ortho_dir):
                if file.endswith('.tif'):
                    image_path = os.path.join(ortho_dir, file)
                    metadata_csv = os.path.join(root, 'metadata.csv')
                    exif_metadata = get_exiftool_metadata(image_path)
                    if os.path.exists(metadata_csv):
                        metadata = process_metadata(metadata_csv)
                        metadata.update(exif_metadata)
                        bbox = get_raster_bbox(image_path)
                        save_metadata_to_file(metadata, ortho_dir, os.path.splitext(file)[0], bbox)
                    else:
                        print(f"No metadata.csv found in {root}. Skipping file {file}.")

if __name__ == "__main__":
    main('/path/to/your/directory')
