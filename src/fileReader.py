"""
PDI Projekt 2023 - ArcGIS stream.
Author: Vojtěch Fiala \<xfiala61\>
"""

from time import sleep, time
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import asyncio
from pyspark.sql.types import IntegerType

class fileReader:
    resPath = "./results/"
    def __init__(this, timeout=10):
        this.timeout = timeout
        sleep(1) # to better synchronize
    
    async def parseFile(this):
        spark = SparkSession.builder.appName("Reader").getOrCreate()
        while True:
            files = [file for file in os.listdir(this.resPath) if file.endswith(".json")]
            if len(files) < 1: # no json present
                sleep(1)
                continue
            file = this.resPath + files[0]
            json_df = spark.read.json(file)
            json_df.show(1000, truncate=False)
            sleep(this.timeout)

    def getLatestResults(this, spark):
        files = [this.resPath + file for file in os.listdir(this.resPath) if file.endswith(".json")]
        if len(files) < 1: # no json present
            sleep(1)
            return
        json_df = spark.read.json(files)

         # partition based on the id and order based on their lastupdate time
        window_spec = Window().partitionBy("id").orderBy(F.col("lastupdate").desc())

        # based on the order, add a row number to each row (because it's descending, the newest one will have 1)
        df_with_row_number = json_df.withColumn("row_number", F.row_number().over(window_spec))

        # drop all but the newest value
        result_df = df_with_row_number.filter(F.col("row_number") == 1).drop("row_number")
        return result_df

    async def parseFilesDefault(this):
        spark = SparkSession.builder.appName("Reader").getOrCreate()
        while True:
            result_df = this.getLatestResults(spark)
            result_df.show(1000, truncate=False)
            sleep(this.timeout)

    async def parseFiles5Delayedest(this):
        spark = SparkSession.builder.appName("Reader").getOrCreate()
        while True:
            result_df = this.getLatestResults(spark)
            result_df = result_df.orderBy("delay", ascending=False).filter(F.col("delay") > 0).limit(5)
            result_df.show(1000, truncate=False)
            sleep(this.timeout)

    async def parseFiles5Delayedest(this):
        spark = SparkSession.builder.appName("Reader").getOrCreate()
        while True:
            result_df = this.getLatestResults(spark)
            result_df = result_df.filter(F.col("lastupdate") > (int(time()*1000)-60*3*1000)).filter(F.col("delay") > 0)
            result_df = result_df.orderBy("lastupdate", ascending=False).limit(5)
            result_df.show(1000, truncate=False)
            sleep(this.timeout)

    async def parseFilesAvgDelay(this):
        spark = SparkSession.builder.appName("Reader").getOrCreate()
        while True:
            result_df = this.getLatestResults(spark)
            result_df = result_df.filter(F.col("lastupdate") > (int(time()*1000)-60*30*1000))

            overall_avg_delay = result_df.agg(F.avg("delay").alias("overall_avg_delay"))

            overall_avg_delay.show()
            sleep(this.timeout)



if __name__ == "__main__":
    a = fileReader()
    asyncio.run(a.parseFilesAvgDelay())
