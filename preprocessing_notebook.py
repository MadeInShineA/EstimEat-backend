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


app._unparsable_cell(
    r"""
    import osmium as o

    class AmenityHandler(o.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.amenities = []

        def node(self, n):
            if \"amenity\" in n.tags:
                self.amenities.append(
                    {
                        \"id\": n.id,
                        \"lat\": n.location.lat,
                        \"lon\": n.location.lon,
                        \"amenity\": n.tags.get(\"amenity\"),
                        \"name\": n.tags.get(\"name\", None),
                    }
                )

    # Parse the .pbf file
    handler = AmenityHandler()
    handler.apply_file(data_path + \"switzerland-251020.osm.pbf\", locations=True)

    # Convert to Polars DataFrame
    osm_df = pl.Dataimport osmium as o

    class AmenityHandler(o.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.amenities = []

        def node(self, n):
            if \"amenity\" in n.tags:
                self.amenities.append(
                    {
                        \"id\": n.id,
                        \"lat\": n.location.lat,
                        \"lon\": n.location.lon,
                        \"amenity\": n.tags.get(\"amenity\"),
                        \"name\": n.tags.get(\"name\", None),
                    }
                )

    # Parse the .pbf file
    handler = AmenityHandler()
    handler.apply_file(data_path + \"switzerland-251020.osm.pbf\", locations=True)

    # Convert to Polars DataFrame
    osm_df = pl.DataFrame(handler.amenities)
    osm_df.head()Frame(handler.amenities)
    osm_df.head()
    """,
    name="_"
)


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
def _(localities_df):
    print(localities_df.crs)
    return


@app.cell
def _(localities_df, restaurant_gdf):
    restaurant_gdf_crs = restaurant_gdf.to_crs(localities_df.crs)
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
    result = joined[["id", "lat", "lon", "amenity", "name_right"]]
    return (result,)


@app.cell
def _(pl, result):
    df = (
        pl.from_pandas(result)
        .rename({"name_right": "locality"})
        .group_by("locality")
        .agg(pl.len().alias("restaurant_count")).sort("restaurant_count", descending=True)
    )

    df.head()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
