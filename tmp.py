from time import sleep, time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, max, max_by
from pyspark.sql.window import Window
import asyncio
from pyspark.sql.types import IntegerType

# Method to filter only the latest results from the data found using "complete"
def getLatestResults(stream, tmpCol):
        
        # group by the attributes I want, then perform a dummy aggregation and order by the lastupdate
        latest = stream.groupBy(["attributes.id", "attributes.lastupdate", tmpCol]).agg(first("attributes.bearing")).orderBy(col("attributes.lastupdate").desc())

        # now group by ID and use aggregation to get the latest update time and latest stop ID (by using first since its sorted)
        most_recent = latest.groupBy("id").agg(max("lastupdate"), first(tmpCol.replace("attributes.", "")))

        return most_recent

def parseFile():
    spark = SparkSession.builder.appName("Reader").getOrCreate()
    files = ["test.json"]

    json_df = spark.read.json(files)

    latest  = json_df.groupBy(["attributes.id"]).agg(max("attributes.lastupdate"), max_by("attributes.laststopid", "attributes.lastupdate"))


    latest.show(1000, truncate=False)


parseFile()