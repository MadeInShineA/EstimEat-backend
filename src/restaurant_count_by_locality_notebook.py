import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import geopandas as gpd
    import matplotlib.pyplot as plt
    import pandas as pd
    from shapely.geometry import Point
    return gpd, pl, plt


@app.cell
def _():
    data_path = "./data/"
    return (data_path,)


@app.cell
def _(data_path, gpd):
    layers = gpd.list_layers(data_path + "localities.gpkg")
    print(layers)
    return


@app.cell
def _(data_path, gpd):
    localities_df = gpd.read_file(
        data_path + "localities.gpkg", layer="tlm_hoheitsgebiet"
    )
    localities_df.info()
    return (localities_df,)


@app.cell
def _(localities_df, plt):
    # Plot
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))
    localities_df.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=0.3)

    # Optional: Add title
    ax.set_title("Swiss Municipalities (TLM Hoheitsgebiet)", fontsize=14)

    # Remove axes for cleaner look
    ax.set_axis_off()

    plt.show()
    return


@app.cell
def _(data_path, pl):
    import osmium as o
    import re
    from pathlib import Path

    class AmenityHandler(o.SimpleHandler):
        def __init__(self, year: int):
            super().__init__()
            self.year = year
            self.amenities = []

        def node(self, n):
            if "amenity" in n.tags:
                # Skip nodes without valid coordinates
                if not n.location.valid():
                    return
                self.amenities.append({
                    "id": n.id,
                    "lat": n.location.lat,
                    "lon": n.location.lon,
                    "amenity": n.tags.get("amenity"),
                    "name": n.tags.get("name", None),
                    "year": self.year,
                })

    osm_dir = Path(data_path) / "osm"

    osm_files =  [f for f in osm_dir.iterdir() if f.is_file()]


    all_osm_dfs = []

    for osm_file in osm_files:
        filename = Path(osm_file).name
        match = re.search(r"-(\d{2})", filename)
        if not match:
            print(f"⚠️  Could not extract year from filename: {filename}")
            continue
        year = 2000 + int(match.group(1))
        # Parse amenities with fixed year
        handler = AmenityHandler(year=year)
        handler.apply_file(osm_file, locations=True)

        if handler.amenities:
            osm_file_df = pl.DataFrame(handler.amenities)
            all_osm_dfs.append(osm_file_df)
            print(f"✅ Loaded {len(handler.amenities)} amenities from {filename} (year={year})")
        else:
            print(f"ℹ️  No amenities found in {filename}")

    # Combine all into one DataFrame
    if all_osm_dfs:
        osm_df = pl.concat(all_osm_dfs, how="vertical")
        print("\nFinal DataFrame:")
        print(osm_df.head())
    else:
        raise ValueError("No data loaded from any PBF file.")
    return Path, osm_df


@app.cell
def _(osm_df, pl):
    restaurant_df = osm_df.filter(pl.col("amenity") == "restaurant")
    restaurant_df.head()
    return (restaurant_df,)


@app.cell
def _(restaurant_df):
    restaurant_df.schema
    return


@app.cell
def _(gpd, restaurant_df):
    restaurant_gdf = gpd.GeoDataFrame(
        restaurant_df.to_pandas(),  # or use your actual DataFrame source
        geometry=gpd.points_from_xy(restaurant_df["lon"], restaurant_df["lat"]),
        crs="EPSG:4326",  # WGS84
    )

    restaurant_gdf.isna().sum()
    return (restaurant_gdf,)


@app.cell
def _(restaurant_gdf):
    restaurant_gdf_cleaned = restaurant_gdf.dropna(subset=['name'])
    return (restaurant_gdf_cleaned,)


@app.cell
def _(localities_df):
    print(localities_df.crs)
    return


@app.cell
def _(localities_df, restaurant_gdf_cleaned):
    restaurant_gdf_crs = restaurant_gdf_cleaned.to_crs(localities_df.crs)
    return (restaurant_gdf_crs,)


@app.cell
def _(restaurant_gdf_crs):
    restaurant_gdf_crs.head()
    return


@app.cell
def _(gpd, localities_df, restaurant_gdf_crs):
    joined = gpd.sjoin(
        restaurant_gdf_crs,
        localities_df[["name", "geometry"]],  # only keep needed columns
        how="left",
        predicate="within",  # or 'intersects' if points may be on borders
    )
    return (joined,)


@app.cell
def _(joined):
    result = joined[["id", "lat", "lon", "amenity", "year", "name_right"]]
    return (result,)


@app.cell
def _(pl, result):
    df = (
        pl.from_pandas(result)
        .rename({"name_right": "locality"})
        .drop_nulls(subset=["locality"])
        .group_by("locality", "year")
        .agg(pl.len().alias("restaurant_count"))
        .sort(["locality", "year"], descending=[False, True])
    )

    df.head()
    return (df,)


@app.cell
def _(Path, df):
    output_dir = Path("res")
    output_dir.mkdir(parents=True, exist_ok=True)

    df.write_csv(output_dir / "restaurant_count_by_locality.csv")
    return


if __name__ == "__main__":
    app.run()
