"""
 schema.py
 Module for Schema
"""
from pyspark.sql.types import *


def authors_schema():
    """
     Returns Author Details Schema
    :return: StructType
    """

    schema = StructType([
        StructField('last_modified', StructType([
            StructField('type', StringType(), True),
            StructField('value', StringType(), True)
        ])),
        StructField('created', StructType([
            StructField('type', StringType(), True),
            StructField('value', StringType(), True)
        ])),
        StructField('birth_date', StringType(), True),
        StructField('death_date', StringType(), True),
        StructField('key', StringType(), True),
        StructField('name', StringType(), True),
        StructField('personal_name', StringType(), True),
        StructField('revision', LongType(), True),
        StructField('type', StructType([
            StructField('key', StringType(), True)
        ]))
    ])
    return schema
