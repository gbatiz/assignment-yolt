import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as fn


APP_NAME = "Marketing: Organic rolling share"


def calculate_rolling_share_of_organic_acquisition_per_country(
    data_source,
    output_uri,
):
    """
    Calculates the share of organic channel in user acquisitions per country,
    over a rolling window of the past 4 weeks.

    :param data_source: The URI of acquisition aggregates in parquet format
    :param output_uri: The URI where output is written
    """

    assert data_source, "Must provide input data via `data_source`-argument."
    assert output_uri, "Must provide target via `output_uri`-argument."

    with SparkSession.builder.appName(APP_NAME).getOrCreate() as spark:

        window_four_weeks = (
            Window().partitionBy("country").orderBy("period").rangeBetween(-3, 0)
        )

        year_week_to_int = fn.udf(
            lambda year_week: int(year_week.replace("-", "")), IntegerType()
        )

        (
            spark.read.parquet(data_source)
            .filter((fn.col("os_name") == "all") & (fn.col("country") != "ALL"))
            .select(
                [
                    (fn.col("name") == "Organic").alias("is_organic"),
                    "value",
                    "year_week",
                    year_week_to_int(fn.col("year_week")).alias("period"),
                    "country",
                ]
            )
            .groupby(["year_week", "period", "country"])
            .pivot("is_organic")
            .sum("value")
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
            .write.parquet(
                output_uri,
                partitionBy="country",
                mode="overwrite",
                compression="snappy",
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_source",
        help="The URI for you CSV restaurant data, like an S3 bucket location.",
    )
    parser.add_argument(
        "--output_uri",
        help="The URI where output is saved, like an S3 bucket location.",
    )
    args = parser.parse_args()

    calculate_rolling_share_of_organic_acquisition_per_country(
        args.data_source, args.output_uri
    )
