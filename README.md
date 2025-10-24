# EstimEat Backend

The data analysis backend for the EstimEat website, providing intelligent insights into restaurant eEstimEastablishment potential across Swiss municipalities.

## Overview

EstimEat Backend is the analytical engine powering the EstimEat platform. It processes and analyzes comprehensive socio-economic datasets to compute a composite potential score for each Swiss commune (municipality), helping entrepreneurs and investors identify optimal locations for new restaurant ventures.

The system combines multiple indicators including demographic trends, building activity, existing restaurant density, and third-sector economic indicators to provide data-driven recommendations for restaurant placement.

## Integration with EstimEat Website

The processed scores and analytical outputs from this backend power the EstimEat web platform, enabling:

- **Interactive Location Mapping**: Visual exploration of potential scores across Swiss municipalities
- **Detailed Analytics**: Breakdown of contributing socio-economic factors for each location
- **Smart Filtering**: Advanced search and filtering to find optimal restaurant locations
- **Trend Visualization**: Charts and graphs showing demographic and economic trends
- **Data-Driven Insights**: Evidence-based recommendations for restaurant entrepreneurs and investors

## Features

- **Data Processing**: Automated processing of multiple data sources including demographics, building permits, restaurant locations, and economic indicators
- **Score Normalization**: Intelligent normalization of trend scores while preserving sign (positive/negative trends)
- **Composite Scoring**: Calculation of total potential scores by combining normalized indicators
- **Geospatial Analysis**: Integration of OpenStreetMap data with Swiss municipality boundaries
- **Interactive Notebooks**: Marimo-based reactive notebooks for data exploration and analysis

## Installation

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager (install with `pip install uv` or follow [official installation guide](https://github.com/astral-sh/uv#installation))

### Setup

1. Clone the repository:

```bash
git clone <repository-url>
cd EstimEat-backend
```

2. Install dependencies and create virtual environment:

```bash
uv sync
```

The `uv sync` command will automatically create a `.venv` virtual environment and install all dependencies as specified in `pyproject.toml` and locked in `uv.lock`.

## Usage

### Prerequisites

Ensure all required data files are placed in the `data/` directory:

- Swiss municipality boundaries shapefile
- OpenStreetMap PBF files for restaurant data
- Socio-economic datasets in CSV format

### Running the Analysis Pipeline

Execute the analysis notebooks in the following order to process data and generate scores:

1. **Demographics Analysis** (`src/demography_per_locality.ipynb`)
   - Processes population and demographic trend data

2. **Building Trends** (`src/new_building_per_locality.ipynb`)
   - Analyzes construction permits and building activity

3. **Restaurant Counting** (`src/restaurant_count_by_locality_notebook.py`)
   - Extracts and counts restaurant locations from OSM data
   - Run with: `python src/restaurant_count_by_locality_notebook.py`

4. **Third Sector Analysis** (`src/third_sector_scores_per_locality_notebook.py`)
   - Evaluates service sector jobs and establishments

5. **Final Scoring** (`src/final_score_and_normalization.ipynb`)
   - Normalizes all scores and computes composite totals

### Output Files

After running the pipeline, the following files will be generated in `res/`:

- `*_norma.csv`: Normalized scores for each indicator
- `scores_final_par_commune.csv`: Final composite scores for all municipalities

### Data Sources

The analysis requires the following data files (place in `data/` directory):

- **Swiss Municipality Boundaries**: `swissboundaries3d_2024-01_2056_5728.shp` (from Swiss Federal Office of Topography)
- **OpenStreetMap Data**: `.osm.pbf` files in `data/osm/` containing restaurant amenity data
- **Demographic Data**: Population statistics and trends by municipality
- **Economic Indicators**: Third-sector employment and establishment data
- **Building Permits**: Construction activity data by locality

All data should be current and cover consistent time periods for accurate trend analysis.

### Output

The final output is `scores_final_par_commune.csv` containing:

- `commune`: Municipality name
- `third_sector_job_score`: Normalized third-sector job trend score
- `building_score`: Normalized building activity score
- `demographie_score`: Normalized demographic trend score
- `restau_score`: Normalized restaurant trend score
- `third_sector_establishment_score`: Normalized third-sector establishment score
- `total`: Composite potential score (higher = better potential)

## Project Structure

```
EstimEat-backend/
├── src/                          # Source code and notebooks
│   ├── demography_per_locality.ipynb
│   ├── final_score_and_normalization.ipynb
│   ├── new_building_per_locality.ipynb
│   ├── restaurant_count_by_locality_notebook.py
│   └── third_sector_scores_per_locality_notebook.py
├── res/                          # Processed data outputs
│   ├── *_norma.csv              # Normalized score files
│   └── scores_final_par_commune.csv
├── data/                         # Raw data inputs (not included)
├── pyproject.toml               # Project configuration
├── uv.lock                      # Dependency lock file
└── README.md
```

## Dependencies

Key dependencies include:

- **polars**: High-performance DataFrame operations
- **geopandas**: Geospatial data processing
- **marimo**: Reactive Python notebooks
- **matplotlib**: Data visualization
- **osmium**: OpenStreetMap data parsing
- **pyarrow**: Efficient data serialization

## Methodology

### Data Processing Pipeline

1. **Data Collection**: Gather socio-economic indicators from various sources including Swiss statistical offices, OpenStreetMap, and economic databases
2. **Geospatial Analysis**: Process restaurant locations and municipality boundaries using GeoPandas
3. **Trend Calculation**: Compute year-over-year changes for each indicator
4. **Score Normalization**: Standardize all metrics to comparable scales
5. **Composite Scoring**: Combine normalized indicators into final potential scores

### Score Normalization

Each indicator undergoes sign-preserving normalization to a [-10, 10] scale:

- **Positive Trends**: Scaled proportionally to [0, 10] where 10 represents the strongest positive trend
- **Negative Trends**: Scaled proportionally to [-10, 0] where -10 represents the strongest negative trend
- **Zero Values**: Remain unchanged, indicating no trend

This approach maintains the relative strength of trends while ensuring comparability across different types of indicators.

### Composite Score Calculation

The total potential score is calculated as the sum of all normalized component scores:

```
Total Score = Building Score + Demographic Score + Restaurant Score + Third Sector Job Score + Third Sector Establishment Score
```

Higher total scores indicate greater potential for successful restaurant establishments, with each component contributing insights into different aspects of market attractiveness.

### Score Interpretation Guide

- **Building Score**: Reflects construction activity and urban development trends
- **Demographic Score**: Indicates population growth and demographic shifts
- **Restaurant Score**: Shows existing restaurant density and competition levels
- **Third Sector Scores**: Represent economic activity in services and commerce

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License

[Add license information if applicable]

