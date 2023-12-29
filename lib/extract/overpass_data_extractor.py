import json
import os
from urllib.parse import quote

import requests

from tracking_decorator import TrackingDecorator

points_of_interest_queries = [
    # Health
    {"name": "doctors", "type": "node", "query": "amenity=doctors"}
]


@TrackingDecorator.track_time
def extract_overpass_data(source_path, results_path, clean=False, quiet=False):
    # Make results path
    os.makedirs(os.path.join(results_path), exist_ok=True)

    # Define bounding box
    bounding_box = [13.088333218007715, 52.33824183586156, 13.759587218876971, 52.67491714954712]

    # Iterate over queries
    for points_of_interest_query in points_of_interest_queries:
        name = points_of_interest_query["name"]
        type = points_of_interest_query["type"]
        query = points_of_interest_query["query"]

        # Query Overpass API
        overpass_json = extract_overpass_json(type, query, bounding_box[0], bounding_box[1], bounding_box[2],
                                              bounding_box[3])

        # Write json file
        write_json_file(
            file_path=os.path.join(results_path, "berlin-lor-points-of-interest",
                                   f"berlin-lor-points-of-interest-{name}-details.json"),
            query_name=name,
            json_content=overpass_json,
            clean=clean,
            quiet=quiet
        )


def read_geojson_file(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as geojson_file:
        return json.load(geojson_file, strict=False)


def extract_overpass_json(type, query, xmin, ymin, xmax, ymax):
    try:
        data = f"""
[out:json][timeout:25];
(
  {type}[{query}]({ymin}, {xmin}, {ymax}, {xmax});
);
out geom;
"""
        formatted_data = quote(data.lstrip("\n"))

        url = f"https://overpass-api.de/api/interpreter?data={formatted_data}"
        response = requests.get(url)
        text = response.text.replace("'", "")
        return json.loads(text)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")
        return None


def write_json_file(file_path, query_name, json_content, clean, quiet):
    if not os.path.exists(file_path) or clean:

        # Make results path
        path_name = os.path.dirname(file_path)
        os.makedirs(os.path.join(path_name), exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(json_content, json_file, ensure_ascii=False)

            if not quiet:
                print(f"✓ Extract data for {query_name} into {os.path.basename(file_path)}")
    else:
        print(f"✓ Already exists {os.path.basename(file_path)}")
