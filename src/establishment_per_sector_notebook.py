import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import openpyxl
    return (pl,)


@app.cell
def _(pl):
    df = pl.read_csv(
        "./data/establishment_per_sector.csv",
        encoding="latin1",  # évite l’erreur en remplaçant les octets invalides
        separator=";",
        try_parse_dates=True,
        infer_schema_length=10000,
    )

    df = df.rename({
        "Année": "year",
        "Commune": "locality",
        "Secteur économique": "sector",
        "Etablissements": "establishments",
        "Emplois": "jobs"
    })
    return (df,)


@app.cell
def _(df):
    df.shape
    return


@app.cell
def _(df):
    df.head()
    return


@app.cell
def _(df, pl):
    df_cleaned = df.filter(
        (pl.col("locality") != "Suisse") & 
        (~pl.col("sector").str.contains("total"))
    )

    sector_map = {"Secteur primaire": 1, "Secteur secondaire": 2, "Secteur tertiaire": 3}

    df_cleaned = df_cleaned.with_columns(
        pl.col("sector")
        .replace({
            "Secteur primaire": 1,
            "Secteur secondaire": 2,
            "Secteur tertiaire": 3
        })
        .cast(pl.Int8),
    
        pl.col("establishments").replace({
            "X": None,
            "0": None
        }).cast(pl.Int64),
    
        pl.col("jobs").replace({
            "X": None,
            "0": None
        }).cast(pl.Int64),

        pl.col("locality").str.replace(r"^\d+\s*", "", literal=False)
    )

    df_cleaned = df_cleaned.filter(
        (pl.col("sector") == 3)
    ).drop("sector").sort(["locality", "year"])

    df_cleaned.head()
    return (df_cleaned,)


@app.cell
def _(df_cleaned):
    df_establishments = df_cleaned.drop("jobs")

    df_establishments.head()
    return (df_establishments,)


@app.cell
def _(df_establishments):
    df_establishments.null_count()
    return


@app.cell
def _(df_cleaned):
    df_jobs = df_cleaned.drop("establishments")

    df_jobs.head()
    return (df_jobs,)


@app.cell
def _(df_jobs):
    df_jobs.null_count()
    return


@app.cell
def _(df_establishments, df_jobs, pl):
    df_establishments_null_filled = (
        df_establishments
        .group_by("locality", maintain_order=True)
        .map_groups(lambda g: g.with_columns(pl.col("establishments").interpolate()))
    )


    df_jobs_null_filled = (
        df_jobs
        .group_by("locality", maintain_order=True)
        .map_groups(lambda g: g.with_columns(pl.col("jobs").interpolate()))
    )
    return df_establishments_null_filled, df_jobs_null_filled


@app.cell
def _(df_establishments, pl):
    df_establishments.filter(pl.col("locality") == "Beinwil (SO)")
    return


@app.cell
def _(df_establishments_null_filled, pl):
    df_establishments_null_filled.filter(pl.col("locality") == "Beinwil (SO)")
    return


@app.cell
def _(df_establishments_null_filled, df_jobs_null_filled, pl):
    max_year_threshold = 2023

    df_establishments_cleaned = df_establishments_null_filled.drop_nulls()

    df_establishments_locality_to_remove = df_establishments_cleaned.group_by(
        pl.col("locality")).agg(pl.col("year").max()).filter(pl.col("year") < max_year_threshold).select("locality").to_series().to_list()

    df_establishments_cleaned = df_establishments_cleaned.filter(~pl.col("locality").is_in(df_establishments_locality_to_remove))

    df_jobs_cleaned = df_jobs_null_filled.drop_nulls()

    df_jobs_locality_to_remove = df_jobs_cleaned.group_by(pl.col("locality")).agg(pl.col("year").max()).filter(pl.col("year") < max_year_threshold).select("locality").to_series().to_list()

    df_jobs_cleaned = df_jobs_cleaned.filter(~pl.col("locality").is_in(df_jobs_locality_to_remove))
    return df_establishments_cleaned, df_jobs_cleaned


@app.cell
def _(df_establishments_cleaned, pl):
    df_establishments_growth = (
        df_establishments_cleaned
        .with_columns([
            pl.col("establishments").shift(1).over(["locality"]).alias("last_year_estabs"),
        ])
        .with_columns([
            ((pl.col("establishments") - pl.col("last_year_estabs")) / pl.col("last_year_estabs")).alias("estab_growth"),
        ])
        .filter(pl.col("year") > pl.col("year").min().over(["locality"])).drop("last_year_estabs", "establishments")
    )

    df_establishments_growth.head()
    return (df_establishments_growth,)


@app.cell
def _(df_jobs_cleaned, pl):
    df_jobs_growth = (
        df_jobs_cleaned
        .with_columns([
            pl.col("jobs").shift(1).over(["locality"]).alias("last_year_jobs"),
        ])
        .with_columns([
            ((pl.col("jobs") - pl.col("last_year_jobs")) / pl.col("last_year_jobs")).alias("job_growth"),
        ])
        .filter(pl.col("year") > pl.col("year").min().over(["locality"])).drop("last_year_jobs", "jobs")
    )

    df_jobs_growth.head()
    return


@app.cell
def _(df_establishments_growth):
    df_establishments_growth.schema
    return


@app.cell
def _(df_establishments_growth, pl):
    DECAY = 0.2
    PRIOR_WEIGHT = 3

    df_with_weights = (
        df_establishments_growth
        .with_columns(
            max_year=pl.col("year").max().over("locality"),
        )
        .with_columns(
            weight=(-(DECAY * (pl.col("max_year") - pl.col("year")))).exp()
        )
        .drop("max_year")
    )

    global_avg = (
        df_with_weights
        .group_by("locality")
        .agg(
            ws=(pl.col("weight") * pl.col("estab_growth")).sum() / pl.col("weight").sum()
        )["ws"].mean()
    )

    df_establishment_score = (
        df_with_weights
        .group_by("locality")
        .agg(
            weight_sum=pl.col("weight").sum(),
            raw_score=(pl.col("weight") * pl.col("estab_growth")).sum() / pl.col("weight").sum(),
        )
        .with_columns(
            bayes_score=(
                pl.col("raw_score") * pl.col("weight_sum") + global_avg * PRIOR_WEIGHT
            ) / (pl.col("weight_sum") + PRIOR_WEIGHT)
        )
        .select("locality", "bayes_score")
        .sort("bayes_score", descending=True)
    )

    print("Global average:", global_avg)
    print(df_establishment_score.head())

    return df_establishment_score, df_with_weights


@app.cell
def _(df_establishment_score, pl):
    df_establishment_score.filter(pl.col("locality") == "Montet (Glâne) ")

    return


@app.cell
def _(df_establishments_cleaned, pl):
    df_establishments_cleaned.filter(pl.col("locality") == "Montet (Glâne)")
    return


@app.cell
def _(df_establishments_growth, pl):
    df_establishments_growth.filter(pl.col("locality") == "Montet (Glâne)")
    return


@app.cell
def _(df_with_weights, pl):
    df_la = df_with_weights.filter(pl.col("locality") == "L'Abergement")

    raw_weighted_score = (
        (df_la["weight"] * df_la["estab_growth"]).sum() / df_la["weight"].sum()
    )

    print("Raw weighted score for L'Abergement:", raw_weighted_score)

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
