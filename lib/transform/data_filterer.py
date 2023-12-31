import os

import pandas as pd

from lib.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def filter_data(source_path, results_path, clean=False, quiet=False):
    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):

        # Make results path
        subdir = subdir.replace(f"{source_path}/", "")
        os.makedirs(os.path.join(results_path, subdir), exist_ok=True)

        for file_name in [file_name for file_name in sorted(files) if file_name.endswith("-details.csv")]:
            source_file_path = os.path.join(source_path, subdir, file_name)

            drop_unassigned_lor_area_id(source_file_path, quiet=quiet)


def drop_unassigned_lor_area_id(source_file_path, quiet):
    dataframe = read_csv_file(source_file_path)
    dataframe.drop(dataframe[dataframe["planning_area_id"] == 0].index, inplace=True)

    # Write csv file
    dataframe.to_csv(source_file_path, index=False)
    if not quiet:
        print(f"✓ Drop unassigned LOR area IDs {os.path.basename(source_file_path)}")


def read_csv_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as csv_file:
            return pd.read_csv(csv_file, dtype={"id": "str"})
    else:
        return None
