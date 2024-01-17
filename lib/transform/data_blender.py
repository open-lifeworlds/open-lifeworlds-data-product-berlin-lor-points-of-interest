import json
import os
import re

import pandas as pd

from lib.tracking_decorator import TrackingDecorator

key_figure_group = "berlin-lor-points-of-interest"

statistic_properties = [
    # Residential Areas
    # "housing_complexes",
    # "apartment_buildings",

    # Workplaces
    "offices",
    "coworking_spaces",

    # Commercial Services
    "supermarkets",
    "grocery_stores",
    "convenience_stores",
    # "markets",

    # Education
    "schools",
    "kindergartens",
    "childcare",
    "libraries",

    # Healthcare
    "doctors",
    "pharmacies",
    "clinics",

    # Recreation and Leisure
    "sport_centers",
    "fitness_centers",

    # Cultural Spaces
    "art_galleries",
    "theaters",
    "museums",
    "cinemas",

    # Food and Dining
    "cafes",
    "restaurants",
    "marketplaces",
    "bars",
    "pubs",
    "beer_gardens",
    "fast_food_restaurants",
    "food_courts",
    "ice_cream_parlours",
    "nightclubs",

    # Public Services
    "post_offices",
    "police_stations",
    "fire_stations",

    # Transportation
    "bus_stops",
    "ubahn_stops",
    "sbahn_stops",
    "tram_stops",
    "bicycle_rentals",
    "car_sharing_stations",

    # Community Spaces
    "community_centers",
    "places_of_worship",

    # Green Spaces
    # "parks", # TODO Find a way to count parks
    # "urban_gardens",
    # "greenfield",
    # "grass",
]


@TrackingDecorator.track_time
def blend_data(source_path, results_path, clean=False, quiet=False):
    # Make results path
    os.makedirs(os.path.join(results_path), exist_ok=True)

    # Load population statistics
    json_statistics_population = load_population_statistics(
        os.path.join(os.path.join(source_path, "berlin-lor-population-statistics"),
                     f"berlin-lor-population-statistics.json"))

    # Initialize statistics
    json_statistics = {}

    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):

        statistics_name = subdir.replace(f"{source_path}/", "")

        if statistics_name.startswith("berlin-lor-points-of-interest-2"):

            # Load statistics
            csv_statistics = read_csv_file(os.path.join(source_path, statistics_name, f"{statistics_name}.csv"))

            year = re.search(r"\b\d{4}\b", statistics_name).group()
            month = re.search(r"\b\d{2}(?<!\d{4})\b", statistics_name).group()

            # Iterate over LOR area types
            for lor_area_type in ["districts", "forecast-areas", "district-regions", "planning-areas"]:

                # Load geojson
                if lor_area_type == "districts":
                    geojson = read_geojson_file(
                        os.path.join(source_path, "berlin-lor-geodata-geojson", f"berlin-lor-{lor_area_type}.geojson"))
                elif int(year) <= 2020:
                    geojson = read_geojson_file(
                        os.path.join(source_path, "berlin-lor-geodata-geojson",
                                     f"berlin-lor-{lor_area_type}-until-2020.geojson"))
                elif int(year) >= 2021:
                    geojson = read_geojson_file(
                        os.path.join(source_path, "berlin-lor-geodata-geojson",
                                     f"berlin-lor-{lor_area_type}-from-2021.geojson"))
                else:
                    geojson = None

                # Extend geojson
                extend(
                    year=year,
                    month=month,
                    geojson=geojson,
                    statistics_name=statistics_name,
                    csv_statistics=csv_statistics,
                    json_statistics=json_statistics,
                    json_statistics_population=json_statistics_population
                )

                # Write geojson file
                write_geojson_file(
                    file_path=os.path.join(results_path, statistics_name,
                                           f"{key_figure_group}-{year}-{month}-{lor_area_type}.geojson"),
                    statistic_name=f"{key_figure_group}-{year}-{month}-{lor_area_type}",
                    geojson_content=geojson,
                    clean=clean,
                    quiet=quiet
                )

    # Write json statistics file
    write_json_file(
        file_path=os.path.join(results_path, f"{key_figure_group}-statistics",
                               f"{key_figure_group}-statistics.json"),
        statistic_name=f"{key_figure_group}-statistics",
        json_content=json_statistics,
        clean=clean,
        quiet=quiet
    )


def extend(year, month, geojson, statistics_name, csv_statistics, json_statistics, json_statistics_population):
    """
    Extends geojson and json-statistics by statistical values
    :param year:
    :param month:
    :param geojson:
    :param statistics_name:
    :param csv_statistics:
    :param json_statistics:
    :param json_statistics_population:
    :return:
    """

    # Check for missing files
    if csv_statistics is None:
        print(f"✗️ No data in {statistics_name}")
        return

    # Determine year to take from population statistics
    year_latest = get_latest_year(json_statistics_population)
    year_population = year if year == year_latest else year_latest

    # Iterate over features
    for feature in sorted(geojson["features"], key=lambda feature: feature["properties"]["id"]):
        feature_id = feature["properties"]["id"]
        area_sqm = feature["properties"]["area"]
        area_sqkm = area_sqm / 1_000_000

        inhabitants = get_inhabitants(json_statistics_population, year_population, feature_id)

        # Filter statistics
        statistic_filtered = csv_statistics[csv_statistics["id"].astype(str).str.startswith(feature_id)]

        # Check for missing data
        if statistic_filtered.shape[0] == 0:
            print(f"✗️ No data in {statistics_name} for id={feature_id}")
            continue

        # Blend data
        blend_data_into_feature(feature, statistic_filtered, area_sqkm, inhabitants)
        blend_data_into_json(year, month, feature_id, feature, json_statistics)

    # Calculate averages
    calculate_averages(year, month, year_population, geojson, csv_statistics, json_statistics,
                       json_statistics_population)


def blend_data_into_feature(feature, statistics, area_sqkm, inhabitants):
    # Add new properties
    for property_name in statistic_properties:
        add_property_with_modifiers(feature, statistics, property_name, area_sqkm, inhabitants)

    return feature


def blend_data_into_json(year, half_year, feature_id, feature, json_statistics):
    # Build structure
    if year not in json_statistics:
        json_statistics[year] = {}
    if half_year not in json_statistics[year]:
        json_statistics[year][half_year] = {}

    # Add properties
    json_statistics[year][half_year][feature_id] = feature["properties"]


def calculate_averages(year, half_year, year_population, geojson, csv_statistics, json_statistics,
                       json_statistics_population):
    # Calculate total values
    total_sqkm = get_total_sqkm(geojson)
    total_inhabitants = get_total_inhabitants(year_population, json_statistics_population)

    values = {}

    values_sums = {property_name: int(sum(csv_statistics[property_name])) for property_name in statistic_properties if
                   property_name in csv_statistics}
    values_averages = {}

    if total_sqkm is not None:
        values_averages |= {f"{property_name}_per_sqkm": round(float(total / total_sqkm), 2) for property_name, total in
                            values_sums.items()}
    if total_inhabitants is not None:
        values_averages |= {
            f"{property_name}_per_100k_inhabitant": round(float(total / (total_inhabitants / 100_000)), 2)
            for property_name, total in values_sums.items()}

    values |= values_sums
    values |= values_averages

    json_statistics[year][half_year][0] = values


def add_property(feature, statistics, property_name):
    if statistics is not None and property_name in statistics:
        try:
            feature["properties"][f"{property_name}"] = float(statistics[property_name].sum())
        except ValueError:
            feature["properties"][f"{property_name}"] = 0


def add_property_with_modifiers(feature, statistics, property_name, total_area_sqkm, inhabitants):
    if statistics is not None and property_name in statistics:
        try:
            feature["properties"][f"{property_name}"] = int(statistics[property_name].sum())
            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = round(
                    float(statistics[property_name].sum()) / total_area_sqkm, 2)
            if inhabitants is not None:
                feature["properties"][f"{property_name}_per_100k_inhabitant"] = round(
                    float(statistics[property_name].sum()) / (inhabitants / 100_000), 2)
        except ValueError:
            feature["properties"][f"{property_name}"] = 0

            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = 0
            if inhabitants is not None:
                feature["properties"][f"{property_name}_per_100k_inhabitant"] = 0
        except TypeError:
            feature["properties"][f"{property_name}"] = 0

            if total_area_sqkm is not None:
                feature["properties"][f"{property_name}_per_sqkm"] = 0
            if inhabitants is not None:
                feature["properties"][f"{property_name}_per_100k_inhabitant"] = 0


def load_population_statistics(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as json_file:
        return json.load(json_file, strict=False)


def get_latest_year(population_statistics):
    return max(list(population_statistics.keys()))


def get_inhabitants(population_statistics, year, feature_id):
    try:
        return population_statistics[year]["02"][feature_id]["inhabitants"]
    except KeyError:
        # print(f"✗️ No population data for id={feature_id}")
        return None


def get_total_sqkm(geojson):
    return sum(feature["properties"]["area"] / 1_000_000 for feature in geojson["features"])


def get_total_inhabitants(year, json_statistics_population):
    return json_statistics_population[year]["02"]["0"]["inhabitants"]


def read_csv_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as csv_file:
            return pd.read_csv(csv_file, dtype={"id": "str"})
    else:
        return None


def read_geojson_file(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as geojson_file:
        return json.load(geojson_file, strict=False)


def write_geojson_file(file_path, statistic_name, geojson_content, clean, quiet):
    if not os.path.exists(file_path) or clean:

        # Make results path
        path_name = os.path.dirname(file_path)
        os.makedirs(os.path.join(path_name), exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as geojson_file:
            json.dump(geojson_content, geojson_file, ensure_ascii=False)

            if not quiet:
                print(f"✓ Blend data from {statistic_name} into {os.path.basename(file_path)}")
    else:
        print(f"✓ Already exists {os.path.basename(file_path)}")


def write_json_file(file_path, statistic_name, json_content, clean, quiet):
    if not os.path.exists(file_path) or clean:

        # Make results path
        path_name = os.path.dirname(file_path)
        os.makedirs(os.path.join(path_name), exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(json_content, json_file, ensure_ascii=False)

            if not quiet:
                print(f"✓ Aggregate data from {statistic_name} into {os.path.basename(file_path)}")
    else:
        print(f"✓ Already exists {os.path.basename(file_path)}")
