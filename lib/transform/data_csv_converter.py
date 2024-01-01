import json
import os
from datetime import datetime

import pandas as pd

from lib.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def convert_data_to_csv(source_path, results_path, clean=False, quiet=False):
    timestamp = datetime.now().strftime("%Y-%m")

    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(f"{source_path}-{timestamp}")):

        # Make results path
        subdir = subdir.replace(f"{source_path}/", "")
        os.makedirs(os.path.join(results_path, subdir), exist_ok=True)

        for file_name in [file_name for file_name in sorted(files) if file_name.endswith("-details.json")]:
            source_file_path = os.path.join(source_path, subdir, file_name)
            convert_file_to_csv(source_file_path, clean=clean, quiet=quiet)


def convert_file_to_csv(source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}.csv"

    # Check if result needs to be generated
    if clean or not os.path.exists(file_path_csv):

        # Load json file
        json_file = read_json_file(source_file_path)

        dataframe = pd.DataFrame(json_file["elements"]) \
            .assign(name=lambda df: df["tags"].apply(lambda row: row["name"] if "name" in row else None)) \
            .assign(street=lambda df: df["tags"].apply(lambda
                                                           row: f"{row['addr:street']} {row['addr:housenumber']}" if "addr:street" in row and "addr:housenumber" in row else None)) \
            .assign(
            zip_code=lambda df: df["tags"].apply(lambda row: row["addr:postcode"] if "addr:postcode" in row else None)) \
            .assign(zip_code=lambda df: df["zip_code"].astype(pd.Int64Dtype(), errors="ignore")) \
            .assign(city=lambda df: df["tags"].apply(lambda row: row["addr:city"] if "addr:city" in row else None)) \
            .drop(columns=["type", "tags"])

        try:
            # Write csv file
            dataframe.to_csv(file_path_csv, index=False)
            if not quiet:
                print(f"✓ Convert {os.path.basename(file_path_csv)}")
        except Exception as e:
            print(f"✗️ Exception: {str(e)}")
    elif not quiet:
        print(f"✓ Already exists {os.path.basename(file_path_csv)}")


def read_json_file(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as geojson_file:
        return json.load(geojson_file, strict=False)
