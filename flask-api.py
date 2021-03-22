"""
 flask-api.py
 Flask based API to compute results using Pyspark
"""
from flask import Flask
from flask import request
from flask import Response
from flask_json import FlaskJSON, JsonError, json_response
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import re
import sys
import ingestion.feed_collector
from waitress import serve

app = Flask(__name__)
json = FlaskJSON(app)


@json.encoder
def custom_encoder(o):
    pass


@app.errorhandler(500)
def system_error(e):
    return json_response(status_=500, status=500, description="System error. Please contact administrator !!")


@app.route("/")
def index():
    return app.send_static_file('index.html')


@app.route("/adidas/api/get_total_pages", methods=['POST'])
def get_total_pages():
    data = request.get_json(force=True)
    keys = data.keys()
    output = []
    author = ""
    results = []
    if 'author' in keys:
        author = data['author']
        print("author", author)
        books_data_path = "/adidas/data/cleaned/books/*.parquet"
        authors_data_path = "/adidas/data/cleaned/authors/*.parquet"
        author_key = spark.read.parquet(authors_data_path).filter(col("name") == author).select("author_key").first()[0]
        output = spark.read.parquet(books_data_path).filter(col("author") == author_key)\
            .agg(sum("number_of_pages").alias("total_pages_count"))
        print(output)

    results = output.toPandas().to_json(orient="records")

    return Response(results, mimetype='application/json')


if __name__ == "__main__":
    spark = SparkSession.builder.appName("adidas-case-study-api").getOrCreate()
    serve(app, host="localhost", port="5999")
