import argparse
from itertools import product
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as fn


APP_NAME = "Marketing: Organic rolling share"


# TODO: Write pipeline test for raw data
def test_raw(spark, df):
    pass


# TODO: Write pipeline test for staging data
def test_staging(spark, df):
    pass


def generate_year_week_filler_df(
    startdate: "datetime.date",
    enddate: "datetime.date",
    countries: list
) -> "pyspark.sql.dataframe.DataFrame":

    """Build a Spark Dataframe to use in a join
    for adding rows for weeks which have no data.

    The generated dataframe is like:

    +-------+---------+
    |country|year_week|
    +-------+---------+
    |     GB|  2020_39|
    |     IT|  2020_39|
    |     FR|  2020_39|
    |     GB|  2020_40|
    |     IT|  2020_40|
    |     FR|  2020_40|
    |     GB|  2020_41|
    |     IT|  2020_41|
    |     FR|  2020_41|
    ...

    Parameters
    ----------
    startdate : datetime.date
        The earliest day for which there is data.
    enddate : datetime.date
        The last day for which there is data.
    countries : list
        List of distinct values in the `country`-column.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        The filler dataframe to be used in a broadcast full outer join.
    """

    countries = set(countries) - {"ALL"}

    weeks = pd.date_range(
        start=startdate,
        end=enddate,
        freq='W'
    )

    filler = (
        pd.DataFrame(product(weeks, countries))
        .assign(year_week=lambda df: df[0].dt.strftime("%Y_%W"))
        .rename(columns={1:'country'})
        .drop(columns=[0])
        .pipe(spark.createDataFrame)
    )

    return filler


def transform(
    df: "pyspark.sql.dataframe.DataFrame",
    filler: "pyspark.sql.dataframe.DataFrame"
) -> "pyspark.sql.dataframe.DataFrame":
    """
    Calculates the share of organic channel in user acquisitions per country,
    over a rolling window of the past 4 weeks.

    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Raw data
    filler : pyspark.sql.dataframe.DataFrame
        Spark DF to use for "reindexing" step in transformation
        to add week x country combinations with missing data.
        This is to ensure the moving average is calculated correctly.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Spark DF containing the share of organic channel in user acquisitions
        per country, over a rolling window of the past 4 weeks.
    """

    window_four_weeks = (
        Window().partitionBy("country").orderBy("period").rangeBetween(-3, 0)
    )

    year_week_to_int = fn.udf(
        lambda year_week: int(year_week.replace("_", "")), IntegerType()
    )

    rolling_share_of_organic_acquisition_per_country = (df
        .filter((fn.col("os_name") == "all") & (fn.col("country") != "ALL"))
        .select(
            [
                (fn.col("name") == "Organic").alias("is_organic"),
                "value",
                "year_week",
                "country",
            ]
        )
        .groupby(["year_week", "country"]).pivot("is_organic").sum("value")
        .join(
            other=fn.broadcast(filler),
            on=['year_week', 'country'],
            how='outer'
        )
        .withColumn("period", year_week_to_int(fn.col("year_week")))
        .fillna(0)
        .withColumn("sum_nonorganic_P4W", fn.sum("false").over(window_four_weeks))
        .withColumn("sum_organic_P4W", fn.sum("true").over(window_four_weeks))
        .select(
            [
                fn.split("year_week", "_").getItem(0).cast("int").alias("year"),
                fn.split("year_week", "_").getItem(1).cast("int").alias("week"),
                "country",
                (
                       fn.col("sum_organic_P4W")
                    / (fn.col("sum_organic_P4W") + fn.col("sum_nonorganic_P4W"))
                ).alias("share_organic_p4w"),
            ]
        )
    )

    return rolling_share_of_organic_acquisition_per_country


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_parquet",
        help="The URI of the input data.",
        required=True
    )
    parser.add_argument(
        "--target_parquet",
        help="The URI of the write target for the transformed data .",
        required=True
    )
    args = parser.parse_args()

    with SparkSession.builder.appName(APP_NAME).getOrCreate() as spark:

        raw = spark.read.parquet(args.source_parquet)
        raw.cache()

        test_raw(spark, raw)

        filler = generate_year_week_filler_df(
            *(
                raw.select(
                    fn.min('start_date'),
                    fn.max('start_date'),
                    fn.collect_set('country')
                )
                .toPandas().values.tolist()[0]
            )
        )

        staging = transform(raw, filler).repartition(1)
        staging.cache()

        test_staging(spark, staging)

        staging.write.parquet(
            path=args.target_parquet,
            mode="overwrite",
            compression="snappy",
        )
