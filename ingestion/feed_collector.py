"""
  feed_collector.py
  Python script to load and clean data
"""
from pyspark.sql.functions import *
from common import schema


def load_books_data(spark, path):
    """
    Load Open Library books dump JSON data from given path
    :param spark:
    :param path:
    :return:
    """
    return spark.read.format("json").load(path)


def clean_books_data(spark, data, output_path):
    """
     Clean input data based on below assumptions/filters -
    - Records where both Titles and Authors is null are filtered out
    - Titles ending with "=" or having "+" character are ignored
    - Publish year is calculated to process publish date with format e.g.August 23, 2003
    - Publish year >1950 and Number of Pages > 20 is considered
    - Genres is considered an optional field and having null values. Records with null genres is also included

    :param spark:
    :param data:
    :param output_path:
    :return:
    """
    # Add new column "author" with "authors.key" as StringType
    cleaned_df = data.withColumn("author", explode("authors.key"))

    # Filter all rows where title & author is not null
    cleaned_df = cleaned_df.filter("title is not null and author is not null")

    # Filter all rows where number of pages >20
    cleaned_df = cleaned_df.filter("number_of_pages > 20")

    # Filter Titles ending with "=" character or having "+" character and drop "pubish_date" col
    cleaned_df = cleaned_df.filter(~col("title").rlike("[=$|\\\+]")).filter(
        col("publish_date").rlike("2021|20[0-9]{2}|19[0-9]{2}")).withColumn("publish_year",
                                                                            regexp_extract("publish_date",
                                                                                           "2021|20[0-9]{2}|19[0-9]{2}",
                                                                                           0).cast("Integer")).drop(
        "publish_date")

    # Filter Publish year > 1950 records
    cleaned_df = cleaned_df.filter("publish_year > 1950")

    # Select relevant fields only out of full dtaaset - title,author,publish year,number_of_pages,genres
    cleaned_df = cleaned_df.select(col("title"), col("author"), col("publish_year"), col("number_of_pages"),
                                   col("genres")).distinct()

    # print(cleaned_df.show(truncate=False))
    # cleaned_df.write.parquet("/adidas/data/cleaned_dataset/")
    cleaned_df.coalesce(4).write.parquet(output_path)
    print("################ Cleaned data is saved in path = ", output_path, " as parquet #############")
    return cleaned_df


def process_authors_data(spark, path,output_path):
    """
    Loads Open Library authors tab separated data from given path
    &
    remove records where name, author_key & birth_date is null
    :param output_path:
    :param spark:
    :param path:
    :return:
    """

    authors_df = spark.read.option("sep", r"\t").csv(path).select(col("_c1").alias("author_key"),
                                                            col("_c3").cast("timestamp").alias("timestamp"),
                                                            from_json(col("_c4"), schema.authors_schema()).alias(
                                                                "details")).select(
        col("author_key"), col("timestamp"), col("details.name").alias("name"),
        col("details.birth_date").alias("birth_date"), col("details.death_date").alias("death_date"),
        col("details.personal_name").alias("personal_name"))
    authors_filtered_df = authors_df.filter("birth_date is not null and name is not null and author_key is not null")

    authors_filtered_df.coalesce(4).write.parquet(output_path)
    print("################ Authors Processed data is saved in path = ", output_path, " as parquet #############")
    return authors_filtered_df
