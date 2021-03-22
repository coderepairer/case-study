"""
    main.py
    Python main script to for adidas case study
"""
from pyspark.sql.session import SparkSession
from ingestion import feed_collector
import solutions


def run_solutions():
    """
    Create Spark session, Load Data , Clean up and Run Query Solutions
    :return:
    """
    books_data_path = "/adidas/data/ol_cdump*.json"
    authors_data_path = "/adidas/data/ol_dump_authors*"
    books_output_path = "/adidas/data/cleaned/books/"
    authors_output_path = "/adidas/data/cleaned/authors/"

    print("####################### START ###########################")
    print("################ Running Adidas Case Study Solutions ###########")

    spark = SparkSession.builder.appName("adidas-case-study").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions",4)

    print("################ .... Loading books data ... from path = ", books_data_path,"###########")
    books_data = feed_collector.load_books_data(spark, books_data_path)

    print("################ ... Cleaning books data... ################")
    cleaned_data = feed_collector.clean_books_data(spark, books_data, books_output_path)

    print("################ Solution #1 :: HarryPotter Books #############")
    solutions.query1_solution(spark, cleaned_data)

    print("################ Solution #2 :: Books with the most pages #############")
    solutions.query2_solution(spark, cleaned_data)

    print("################ Solution #3 :: Top 5 authors with most written books #############")
    solutions.query3_solution(spark, cleaned_data)

    print("################ Solution #4 :: Top 5 genres with most books #############")
    solutions.query4_solution(spark, cleaned_data)

    print("################ Solution #5 :: Average number of pages #############")
    solutions.query5_solution(spark, cleaned_data)

    print("################ Solution #6 :: Number of Authors per publish year #############")
    solutions.query6_solution(spark, cleaned_data)

    print("################ .... Processing authors data ... from path = ", authors_data_path,"###########")
    authors_data = feed_collector.process_authors_data(spark, authors_data_path,authors_output_path)

    print("####################### END ###########################")

if __name__ == "__main__":
    run_solutions()
