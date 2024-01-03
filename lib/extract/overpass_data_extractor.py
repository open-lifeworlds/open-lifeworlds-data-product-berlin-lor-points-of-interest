import json
import os
from urllib.parse import quote

import requests

from tracking_decorator import TrackingDecorator

points_of_interest_queries = [
    # Residential Areas
    # {"name": "housing_complexes", "type": "node", "query": "building=residential"},
    # {"name": "apartment_buildings", "type": "node", "query": "building=apartments"},

    # Workplaces
    {"name": "offices", "type": "nwr", "query": "office"},
    {"name": "coworking_spaces", "type": "node", "query": "amenity=coworking_space"},

    # Commercial Services
    {"name": "supermarkets", "type": "node", "query": "shop=supermarket"},
    {"name": "grocery_stores", "type": "node", "query": "shop=grocery"},
    {"name": "convenience_stores", "type": "node", "query": "shop=convenience"},
    # {"name": "markets", "type": "nwr", "query": "shop=market"},

    # Education
    {"name": "schools", "type": "node", "query": "amenity=school"},
    {"name": "kindergartens", "type": "node", "query": "amenity=kindergarten"},
    {"name": "childcare", "type": "node", "query": "amenity=childcare"},
    {"name": "libraries", "type": "node", "query": "amenity=library"},

    # Healthcare
    {"name": "doctors", "type": "node", "query": "amenity=doctors"},
    {"name": "pharmacies", "type": "node", "query": "amenity=pharmacy"},
    {"name": "clinics", "type": "node", "query": "amenity=clinic"},

    # Recreation and Leisure
    {"name": "sport_centers", "type": "node", "query": "leisure=sports_centre"},
    {"name": "fitness_centers", "type": "node", "query": "leisure=fitness_centre"},

    # Cultural Spaces
    {"name": "art_galleries", "type": "node", "query": "tourism=artwork"},
    {"name": "theaters", "type": "node", "query": "amenity=theatre"},
    {"name": "museums", "type": "node", "query": "tourism=museum"},
    {"name": "cinemas", "type": "node", "query": "amenity=cinema"},

    # Food and Dining
    {"name": "cafes", "type": "node", "query": "amenity=cafe"},
    {"name": "restaurants", "type": "node", "query": "amenity=restaurant"},
    {"name": "marketplaces", "type": "node", "query": "amenity=marketplace"},
    {"name": "bars", "type": "node", "query": "amenity=bar"},
    {"name": "pubs", "type": "node", "query": "amenity=pub"},
    {"name": "beer_gardens", "type": "node", "query": "amenity=biergarten"},
    {"name": "fast_food_restaurants", "type": "node", "query": "amenity=fast_food"},
    {"name": "food_courts", "type": "node", "query": "amenity=food_court"},
    {"name": "ice_cream_parlours", "type": "node", "query": "amenity=ice_cream"},
    {"name": "nightclubs", "type": "node", "query": "amenity=nightclub"},

    # Public Services
    {"name": "post_offices", "type": "node", "query": "amenity=post_office"},
    {"name": "police_stations", "type": "node", "query": "amenity=police"},
    {"name": "fire_stations", "type": "node", "query": "amenity=fire_station"},

    # Transportation
    {"name": "bus_stops", "type": "node", "query": "highway=bus_stop"},
    {"name": "ubahn_stops", "type": "node", "query": "railway=station][subway=yes"},
    {"name": "sbahn_stops", "type": "node", "query": "railway=station][light_rail=yes"},
    {"name": "tram_stops", "type": "node", "query": "railway=tram_stop"},
    {"name": "bicycle_rentals", "type": "node", "query": "amenity=bicycle_rental"},
    {"name": "car_sharing_stations", "type": "node", "query": "amenity=car_sharing"},

    # Community Spaces
    {"name": "community_centers", "type": "node", "query": "amenity=community_centre"},
    {"name": "places_of_worship", "type": "node", "query": "amenity=place_of_worship"},

    # Green Spaces
    # {"name": "parks", "type": "nwr", "query": "leisure=park"}, # TODO Find a way to count parks
    # {"name": "urban_gardens", "type": "node", "query": "landuse=allotments"},
    # {"name": "greenfield", "type": "node", "query": "landuse=greenfield"},
    # {"name": "grass", "type": "node", "query": "landuse=grass"},
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
                                   f"berlin-lor-points-of-interest-{name.replace('_', '-')}-details.json"),
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
                print(f"✓ Extract data for {query_name.replace('_', '-')} into {os.path.basename(file_path)}")
    else:
        print(f"✓ Already exists {os.path.basename(file_path)}")
