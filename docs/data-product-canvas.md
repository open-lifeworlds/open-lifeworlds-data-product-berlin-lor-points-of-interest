
# Data Product Canvas - Berlin LOR Points of Interest

## Metadata

* owner: Open Lifeworlds
* description: Data product providing Berlin points-of-interest data on different LOR hierarchy levels
* url: https://github.com/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest
* license: CC-BY 4.0
* updated: 2025-11-06

## Input Ports

### berlin-lor-geodata

* manifest URL: https://raw.githubusercontent.com/open-data-product/open-data-product-berlin-lor-geodata/refs/heads/main/data-product-manifest.yml

### berlin-points-of-interest-source-source-aligned

* manifest URL: https://raw.githubusercontent.com/open-data-product/open-data-product-berlin-points-of-interest-source-aligned/refs/heads/main/data-product-manifest.yml

### berlin-lor-population-source-aligned

* manifest URL: https://raw.githubusercontent.com/open-data-product/open-data-product-berlin-lor-population-source-aligned/refs/heads/main/data-product-manifest.yml

## Output Ports

### berlin-lor-points-of-interest-geojson
name: Berlin Lor Points Of Interest Geojson
* owner: Open Lifeworlds
* url: https://github.com/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/tree/main/data/03-gold/berlin-lor-points-of-interest-geojson
* license: CC-BY 4.0
* updated: 2025-11-06

**Files**

* [berlin-lor-points-of-interest-2025-11-city.geojson](https://media.githubusercontent.com/media/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/refs/heads/main/data/03-gold/berlin-lor-points-of-interest-geojson/berlin-lor-points-of-interest-2025-11-city.geojson)
* [berlin-lor-points-of-interest-2025-11-district-regions.geojson](https://media.githubusercontent.com/media/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/refs/heads/main/data/03-gold/berlin-lor-points-of-interest-geojson/berlin-lor-points-of-interest-2025-11-district-regions.geojson)
* [berlin-lor-points-of-interest-2025-11-districts.geojson](https://media.githubusercontent.com/media/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/refs/heads/main/data/03-gold/berlin-lor-points-of-interest-geojson/berlin-lor-points-of-interest-2025-11-districts.geojson)
* [berlin-lor-points-of-interest-2025-11-forecast-areas.geojson](https://media.githubusercontent.com/media/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/refs/heads/main/data/03-gold/berlin-lor-points-of-interest-geojson/berlin-lor-points-of-interest-2025-11-forecast-areas.geojson)
* [berlin-lor-points-of-interest-2025-11-planning-areas.geojson](https://media.githubusercontent.com/media/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/refs/heads/main/data/03-gold/berlin-lor-points-of-interest-geojson/berlin-lor-points-of-interest-2025-11-planning-areas.geojson)


### berlin-lor-points-of-interest-statistics
name: Berlin Lor Points Of Interest Statistics
* owner: Open Lifeworlds
* url: https://github.com/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/tree/main/data/03-gold/berlin-lor-points-of-interest-statistics
* license: CC-BY 4.0
* updated: 2025-11-06

**Files**

* [berlin-lor-points-of-interest-statistics.json](https://media.githubusercontent.com/media/open-lifeworlds/open-lifeworlds-data-product-berlin-lor-points-of-interest/refs/heads/main/data/03-gold/berlin-lor-points-of-interest-statistics/berlin-lor-points-of-interest-statistics.json)


## Observability

### Quality metrics

 * name: geojson_property_completeness
 * description: The percentage of geojson features that have all necessary properties

| Name | Value |
| --- | --- |
| berlin-lor-points-of-interest-2025-11-city.geojson | 100 |
| berlin-lor-points-of-interest-2025-11-districts.geojson | 100 |
| berlin-lor-points-of-interest-2025-11-forecast-areas.geojson | 100 |
| berlin-lor-points-of-interest-2025-11-district-regions.geojson | 100 |
| berlin-lor-points-of-interest-2025-11-planning-areas.geojson | 100 |


## Classification

**The nature of the exposed data (source-aligned, aggregate, consumer-aligned)**

consumer-aligned


---
This data product canvas uses the template of [datamesh-architecture.com](https://www.datamesh-architecture.com/data-product-canvas).