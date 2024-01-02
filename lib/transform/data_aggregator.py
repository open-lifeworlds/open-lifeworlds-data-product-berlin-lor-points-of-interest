import json
import os
from datetime import datetime
import pandas as pd
from functools import reduce

from lib.tracking_decorator import TrackingDecorator

key_figure_group = "berlin-lor-points-of-interest"

statistics_names = [
    "doctors",
    "pharmacies"
]

@TrackingDecorator.track_time
def aggregate(source_path, results_path, clean=False, quiet=False):
    timestamp = datetime.now().strftime("%Y-%m")

    target_file_path = os.path.join(results_path, f"{key_figure_group}-{timestamp}", f"{key_figure_group}-{timestamp}.csv")

    if not os.path.exists(target_file_path) or clean:

        dataframes_grouped = []

        for statistic_name in statistics_names:

            # Read source file
            source_file_path = os.path.join(source_path, f"{key_figure_group}-{timestamp}",
                                            f"{key_figure_group}-{statistic_name}-{timestamp}-details.csv")
            dataframe_details = read_csv_file(source_file_path)

            dataframe_grouped = dataframe_details.groupby("planning_area_id").agg(
                count=("planning_area_id", "size"),
            ).reset_index() \
                .sort_values(by="planning_area_id") \
                .rename(columns={"planning_area_id": "id"}) \
                .assign(id=lambda df: df["id"].astype(int).astype(str).str.zfill(8)) \
                .assign(count=lambda df: df["count"].astype(pd.Int64Dtype(), errors="ignore")) \
                .rename(columns={"count": statistic_name})
            dataframes_grouped.append(dataframe_grouped)

        dataframe = reduce(lambda left, right: pd.merge(left, right, on="id", how="outer"), dataframes_grouped)
        dataframe.fillna(0, inplace=True)

        # Write csv file
        if dataframe.shape[0] > 0:
            dataframe.to_csv(target_file_path, index=False)
        if not quiet:
            print(f"✓ Summarize into {os.path.basename(target_file_path)}")
        else:
            if not quiet:
                print(dataframe.head())
                print(f"✗️ Empty {os.path.basename(target_file_path)}")
    else:
        print(f"✓ Already summarized into {os.path.basename(target_file_path)}")


def read_csv_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as csv_file:
            return pd.read_csv(csv_file, dtype={"id": "str"})
    else:
        return None


def read_geojson_file(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as geojson_file:
        return json.load(geojson_file, strict=False)
