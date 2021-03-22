"""
  solutions.py
  Adidas OpenLibrary based case study solutions
"""

from pyspark.sql.functions import col, explode, regexp_replace, avg


def query1_solution(spark, cleaned_df):
    """
    Select all "Harry Potter" books
    :param spark:
    :param cleaned_df:
    :return:
    """
    cleaned_df.filter("title like '%Harry Potter%'").orderBy("publish_year").show(truncate=False)


def query2_solution(spark, cleaned_df):
    """
    Get the book with the most pages
    :param spark:
    :param cleaned_df:
    :return:
    """
    cleaned_df.orderBy(col("number_of_pages").desc()).limit(1).show(truncate=False)


def query3_solution(spark, cleaned_df, authors_df):
    """
    Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and
    each row is a different book)
    :param authors_df:
    :param spark:
    :param cleaned_df:
    :return:
    """
    cleaned_df.groupBy("author").count().orderBy(col("count").desc()).limit(5).show(truncate=False)


def query4_solution(spark, cleaned_df):
    """
    Find the Top 5 genres with most books
    :param spark:
    :param cleaned_df:
    :return:
    """
    # "." character in genre is removed before identifying genere counts

    cleaned_df.withColumn("genre", explode("genres")).withColumn("genre", regexp_replace("genre", "\.$", "")).drop(
        "genres").groupBy("genre").count().orderBy(col("count").desc()).limit(5).show(truncate=False)


def query5_solution(spark, cleaned_df):
    """
    Get the avg. number of pages
    :param spark:
    :param cleaned_df:
    :return:
    """
    cleaned_df.agg(avg("number_of_pages").alias("Average")).show()


def query6_solution(spark, cleaned_df):
    """
    Per publish year, get the number of authors that published at least one book
    :param spark:
    :param cleaned_df:
    :return:
    """
    cleaned_df.groupBy("publish_year", "author").count().filter("count >=1").drop("count").groupBy(
        "publish_year").count().orderBy(col("publish_year").desc()).show(100, truncate=False)
