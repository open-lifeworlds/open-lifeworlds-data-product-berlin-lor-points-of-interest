import json
import os
import unittest

file_path = os.path.realpath(__file__)
script_path = os.path.dirname(file_path)

data_path = os.path.join(script_path, "..", "..", "data")

key_figure_group = "berlin-lor-points-of-interest"

statistics_names = [
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
    "places_of_worships",

    # Green Spaces
    # "parks", # TODO Find a way to count parks
    # "urban_gardens",
    # "greenfield",
    # "grass",
]


class FilesTestCase(unittest.TestCase):
    pass


for year in [2024]:
    for month in ["01"]:
        for lor_area_type in ["districts", "forecast-areas", "district-regions", "planning-areas"]:
            file = os.path.join(data_path, f"{key_figure_group}-{year}-{month}",
                                f"{key_figure_group}-{year}-{month}-{lor_area_type}.geojson")
            setattr(
                FilesTestCase,
                f"test_{key_figure_group}_{year}_{month}_{lor_area_type}".replace('-', '_'),
                lambda self, file=file: self.assertTrue(os.path.exists(file))
            )


class PropertiesTestCase(unittest.TestCase):
    pass


for year in [2024]:
    for month in ["01"]:
        for lor_area_type in ["districts", "forecast-areas", "district-regions", "planning-areas"]:
            for statistics_name in statistics_names:
                file = os.path.join(data_path, f"{key_figure_group}-{year}-{month}",
                                    f"{key_figure_group}-{year}-{month}-{lor_area_type}.geojson")
                if os.path.exists(file):
                    with open(file=file, mode="r", encoding="utf-8") as geojson_file:
                        geojson = json.load(geojson_file, strict=False)

                    for feature in geojson["features"]:
                        feature_id = feature["properties"]["id"]
                        setattr(
                            PropertiesTestCase,
                            f"test_{key_figure_group}_{year}_{month}_{lor_area_type}_{feature_id}".replace('-', '_'),
                            lambda self, feature=feature: self.assertTrue(
                                all(property in feature["properties"] for property in statistics_names))
                        )

if __name__ == '__main__':
    unittest.main()
