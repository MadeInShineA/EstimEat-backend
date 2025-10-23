import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import openpyxl
    import matplotlib.pyplot as plt
    return pl, plt


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
    return (df_jobs_growth,)


@app.cell
def _(pl):
    def compute_bayes_score(df, value_col, decay=0.4, prior_weight=3):
        df_weighted = (
            df
            .with_columns(max_year=pl.col("year").max().over("locality"))
            .with_columns(weight=(-decay * (pl.col("max_year") - pl.col("year"))).exp())
            .drop("max_year")
        )

        global_avg = (
            df_weighted
            .group_by("locality")
            .agg(ws=(pl.col("weight") * pl.col(value_col)).sum() / pl.col("weight").sum())
            ["ws"].mean()
        )

        print(f"The global average of ${value_col} is: {global_avg}")

        return (
            df_weighted
            .group_by("locality")
            .agg(
                weight_sum=pl.col("weight").sum(),
                raw_score=(pl.col("weight") * pl.col(value_col)).sum() / pl.col("weight").sum(),
            )
            .with_columns(
                bayes_score=(
                    pl.col("raw_score") * pl.col("weight_sum") + global_avg * prior_weight
                ) / (pl.col("weight_sum") + prior_weight)
            )
            .select("locality", "bayes_score")
            .sort("bayes_score", descending=True)
        )
    return (compute_bayes_score,)


@app.cell
def _(compute_bayes_score, df_establishments_growth):
    df_estabs_score = compute_bayes_score(df_establishments_growth, "estab_growth")
    df_estabs_score.head()
    return (df_estabs_score,)


@app.cell
def _(compute_bayes_score, df_jobs_growth):
    df_jobs_score  = compute_bayes_score(df_jobs_growth, "job_growth")
    df_jobs_score.head()
    return (df_jobs_score,)


@app.cell
def _(pl, plt):
    def plot_best_scores(df_plot_values, value_col, df_scores, number=5):
        # Get top localities
        best_localities = df_scores.head(number).get_column("locality").to_list()

        # Set up the plot
        plt.figure(figsize=(10, 6))

        for i, locality in enumerate(best_localities):
            # Filter data for this locality
            subset = df_plot_values.filter(pl.col("locality") == locality)

            # Extract years and values as NumPy arrays (for reliable plotting)
            years = subset.get_column("year").to_numpy()
            values = subset.get_column(value_col).to_numpy()

            # Sort by year just in case
            sorted_idx = years.argsort()
            years = years[sorted_idx]
            values = values[sorted_idx]

            # Plot with label for legend
            plt.plot(years, values, marker='o', linewidth=2, label=locality)

        # Styling
        plt.title(f"Top {number} Localities by {value_col.replace('_', ' ').title()}", fontsize=14)
        plt.xlabel("Year", fontsize=12)
        plt.ylabel(value_col.replace('_', ' ').title(), fontsize=12)
        plt.legend(title="Locality", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()  # Prevents label cutoff
        plt.show()
    return (plot_best_scores,)


@app.cell
def _(df_establishments_cleaned, df_estabs_score, plot_best_scores):
    plot_best_scores(df_establishments_cleaned, "establishments", df_estabs_score)
    return


@app.cell
def _(df_jobs_cleaned, df_jobs_score, plot_best_scores):
    plot_best_scores(df_jobs_cleaned, "jobs", df_jobs_score)
    return


@app.cell
def _(df_establishments_growth, df_estabs_score, plot_best_scores):
    plot_best_scores(df_establishments_growth, "estab_growth", df_estabs_score)
    return


@app.cell
def _(df_jobs_growth, df_jobs_score, plot_best_scores):
    plot_best_scores(df_jobs_growth, "job_growth", df_jobs_score)
    return


@app.cell
def _(df_estabs_score):
    df_estabs_score.write_csv("./res/third_sector_establishment_score_by_locality.csv", separator=",")
    return


@app.cell
def _(df_jobs_score):
    df_jobs_score.write_csv("./res/third_sector_job_score_by_locality.csv")
    return


if __name__ == "__main__":
    app.run()
