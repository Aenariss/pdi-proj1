"""
PDI Projekt 2023 - spark zpracování dat.
Author: Vojtěch Fiala \<xfiala61\>
"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, first, collect_list, avg, max_by, element_at, from_json, col
import time
import asyncio
import argparse

class dataDownloader:
    test = False
    flag = "north"

    json_schema = StructType([
        StructField("geometry", StructType([
            StructField("x", DoubleType()),
            StructField("y", DoubleType()),
            StructField("spatialReference", StructType([
                StructField("wkid", IntegerType())
            ]))
        ])),
            StructField("attributes", StructType([
            StructField("id", StringType()),
            StructField("vtype", IntegerType()),
            StructField("ltype", IntegerType()),
            StructField("lat", DoubleType()),
            StructField("lng", DoubleType()),
            StructField("bearing", DoubleType()),
            StructField("lineid", IntegerType()),
            StructField("linename", StringType()),
            StructField("routeid", IntegerType()),
            StructField("course", StringType()),
            StructField("lf", StringType()),
            StructField("delay", DoubleType()),
            StructField("laststopid", IntegerType()),
            StructField("finalstopid", IntegerType()),
            StructField("isinactive", StringType()),
            StructField("lastupdate", StringType()),
            StructField("globalid", StringType())
        ]))
    ])
    
    def __init__(this, host, port, flag, test=False):
        this.host = host
        this.port = port
        this.test = test
        this.flag = flag

    def getData(this):
        # when test=true, load data from file instead of stream
        if (this.test):
            this.loadDataFromFiles()
        # Else just downlaod it from source
        else:
            this.downloadData()

        return this.data
    
    def loadDataFromFiles(this):
        return
    
    # Method to filter only the latest results from the data found using "complete" output mode
    def getLatestResults(this, stream, tmpCol):
        
        # group by the attributes I want by ID, then aggregate the maximum update value AND based on the lastudpate value find correspoding searched col
        most_recent = stream.groupBy(["attributes.id"]).agg(max("attributes.lastupdate").alias("lastupdate"), max_by(tmpCol, "attributes.lastupdate").alias(tmpCol.replace("attributes.", "")))

        return most_recent
    
    # Method to use as callback for filtering
    # Checks if value is within last 3 minutes
    def filterThreeMinutes(this, val):
        return val > int(time.time()*1000)-60*1000*3
    
    # Method to write data to output, only every 10 sec because websocket produces result only every 10 sec
    def writeOutput(this, stream, outputMode):
        query = stream.\
            writeStream.format("console").\
            outputMode(outputMode).\
            option("truncate", False).\
            option("numRows", 1000).\
            trigger(processingTime="10 seconds").\
            start()
        return query

    # Method to parse the north option. Finds all vehicles heading north (bearing 0-45, 315-360)
    def parseNorth(this, stream):
        # North is 360, with tolerance 45 that is -> <(315 - 45) % 360, 360> or <0, (360 + 45) % 360>
        onlyNorth = stream.filter(col("attributes.bearing").between(0, 45) | col("attributes.bearing").between(315, 360))

        # Only select ID, bearing and lastupdate columns. I need to run groupby so that I can run aggregation, which is only done because I can't print out
        # results otherwise. Another approach would be to save the output into a file and then use another spark app to read from it,
        # but I'm not sure which to use
        onlySomeCols = onlyNorth.groupby(["attributes.id", "attributes.bearing", "attributes.lastupdate"]).agg(first("attributes.delay"))

        # Drop the unnecessary aggregation
        onlySomeCols = onlySomeCols.drop("first(attributes.delay)")

        if this.test:
            return onlySomeCols
        else:
            # Output the data every 10s
            # Update means only show those that changed (which is everything)
            # Complete would mean show everything it ever recorded which would show thousands of results (good for actual aggregations)
            query = this.writeOutput(onlySomeCols, "update")
            query.awaitTermination()

    # Method to parse the trains option. Finds all trains and their latest known stop ID and update time since the start of the app
    def parseTrains(this, stream):

        # Find columns with vtype == 5, which means its a train
        trains = stream.filter(col("attributes.vtype") == 5)

        latest = this.getLatestResults(trains, "attributes.laststopid")

        if this.test:
            return latest
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(latest, "complete")

            query.awaitTermination()

    # Method to parse the mostdelayed option. Finds the 5 most delayed vehicles since the start of the app
    def parseDelayed(this, stream):
        
        # Latest delay since the app launch
        latest = this.getLatestResults(stream, "attributes.delay")

        # Order by delay, descending, filter where there is any and show only the top 5 most delayed
        latest = latest.orderBy("delay", ascending=False).filter(col("delay") > 0).limit(5)

        if this.test:
            return latest
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(latest, "complete")

            query.awaitTermination()

    # Method to parse the delayed3min option. Find 5 delayed vehicles reported in the last 3 minutes by their lastUpdate value (the most recent is first)
    def parseDelayedInLastThreeMinutes(this, stream):
        
        if this.test:
            max_time = int(stream.agg(max("attributes.lastupdate").alias("maxtime")).first()[0]) # doesn't work with streams... nothing does
            stream = stream.filter(col("attributes.lastupdate") > (max_time - 60*1000*3))
        
        else:
            # Ghetto solution, seems to be working...
            stream = stream.filter(this.filterThreeMinutes(col("attributes.lastupdate")))
        
        stream = stream.filter(col("attributes.delay") > 0) # I only want the delayed

        latest = stream.groupBy("attributes.id").agg(max("attributes.lastupdate").alias("lastupdate"))
        latest = latest.orderBy("lastupdate", ascending=False).limit(5)

        if this.test:
            return latest
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(latest, "complete")

            query.awaitTermination()

    # Method to parse the avgdelay option. Calculates average delay from all reports from the last 3 minutes. 
    def avgDelay(this, stream):

        if this.test:
            max_time = int(stream.agg(max("attributes.lastupdate").alias("maxtime")).first()[0]) # doesn't work with streams... nothing does
            stream = stream.filter(col("attributes.lastupdate") > (max_time - 60*1000*3))
        
        else:
            stream = stream.filter(this.filterThreeMinutes(col("attributes.lastupdate")))
        
        # calculate overall delay
        most_recent = this.getLatestResults(stream, "attributes.delay")
        overall_avg_delay = most_recent.agg(avg("delay").alias("average_delay"))

        if this.test:
            return overall_avg_delay
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(overall_avg_delay, "complete")

            query.awaitTermination()

    # Method to parse avganntime option. Calculates average time between reports for the 10 most recent times
    def avgAnnTime(this, stream):

        # Grouped by IDs and for each ID corresponding last updates in a list and ordered by latest values and get only 10 latest
        most_recent = stream.groupBy("attributes.id").agg(collect_list("attributes.lastupdate").alias("lastupdate")).orderBy(element_at(col("lastupdate"), -1)).limit(10)

        # Find 2 most recent updates and calculate average wait time as newer - older
        dif = most_recent.withColumn("difference", (element_at(col("lastupdate"), -1) - element_at(col("lastupdate"), -2)))

        # Calcualte average difference
        overall_avg = dif.agg(avg("difference").alias("Average time between reports"))

        if this.test:
            return overall_avg
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(overall_avg, "complete")

            query.awaitTermination()

    def downloadData(this):
        if this.flag == "avgdelay" or this.flag == "avganntime":
            spark = SparkSession.builder.appName("PDI Project websocket").config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false").getOrCreate()
        else:
            spark = SparkSession.builder.appName("PDI Project websocket").getOrCreate()

        # Connect to my local stream
        stream = spark.readStream.format("socket").option("host", this.host).option("port", this.port).load()

        # Parse the data as JSON using a given schema
        parsed_stream = stream.select(from_json(col("value"), this.json_schema).alias("json_data")).select("json_data.attributes")
        #stream.coalesce(1).writeStream.outputMode("append").format("json").option("path", "./results").option("checkpointLocation", "./checkpoint").start() # For offline data
        
        # Filter out those that are inactive (isInactive == false)
        parsed_stream = parsed_stream.filter(col("attributes.isinactive") == "false")

        if this.flag == "north": # Vehicles heading north
            this.parseNorth(parsed_stream)
        
        elif this.flag == "trains":
            this.parseTrains(parsed_stream)
        
        elif this.flag == "mostdelayed":
            this.parseDelayed(parsed_stream)

        elif this.flag == "delayed3min":
            this.parseDelayedInLastThreeMinutes(parsed_stream)
        
        elif this.flag == "avgdelay":
            this.avgDelay(parsed_stream)
        
        elif this.flag == "avganntime":
            this.avgAnnTime(parsed_stream)

    def testData(this):
        def readFromFile(file):
            spark = SparkSession.builder.appName("PDI Project websocket").getOrCreate()
            json_df = spark.read.json(file)
        
            # Select only the interesting stuff
            parsed_stream = json_df.select(from_json(col("value"), this.json_schema).alias("json_data")).select("json_data.attributes")

            # Filter out those that are inactive (isInactive == false)
            parsed_stream = parsed_stream.filter(col("attributes.isinactive") == "false")
            return parsed_stream

        
        test_folder = "./tests/"

        if this.flag == "north": # Vehicles heading north
            results = this.parseNorth(readFromFile(test_folder + "north.json"))
            
            print("RECEIVED:")
            results.show()

            results = results.collect()

            print("EXPECTED:")
            # There are only 2 valid vehicles that head north in the file. Check if they're found
            print("id: 7696 | bearing: 45.0 | lastupdate: 1703383549007")
            print("id: 7721 | bearing: 0.0 | lastupdate: 1703383549006")
        
        elif this.flag == "trains":
            results = this.parseTrains(readFromFile(test_folder + "trains.json"))
            print("RECEIVED:")
            results.show()

            results = results.collect()

            print("EXPECTED:")
            # There are 3 train records - two of those are for one train. Checking that only the latest one shows
            print("id: 30361 | lastupdate: 1703383549009 | laststopid: 1147")
            print("id: 30363 | lastupdate: 1703383549007 | laststopid: 1286")
        
        elif this.flag == "mostdelayed":
            results = this.parseDelayed(readFromFile(test_folder + "mostdelayed.json"))
            print("RECEIVED:")
            results.show()

            results = results.collect()

            print("EXPECTED:")
            # There are many delayed records - 5 most delayed have delay of 50 to 46. Check that only the highest show
            # There is also one with 100 delay - but its inactive. Check it dosnt show.
            print("id: 30354 | lastupdate: 1703383549006 | delay: 50.0")
            print("id: 7721 | lastupdate: 1703383549006 | delay: 49.0")
            print("id: 7696 | lastupdate: 1703383549007 | delay: 48.0")
            print("id: 2633 | lastupdate: 1703383549008 | delay: 47.0")
            print("id: 7700 | lastupdate: 1703383549007 | delay: 46.0")

        elif this.flag == "delayed3min":
            results = this.parseDelayedInLastThreeMinutes(readFromFile(test_folder + "delayed3min.json"))

            print("RECEIVED:")
            results.show()

            results = results.collect()

            print("EXPECTED:")
            # There are many delayed records - when testing, I use the highest timestamp found as current time.
            # The latest ones should have different ;lastupdate by 30000 each - that means 30 seconds and should end in 9 8 7.
            # There should be only 3 of them -  the other ones shuld be earlier than 3 minutes before
            print("id: 30354 | lastupdate: 1703383609009")
            print("id: 7721 | lastupdate: 1703383579008")
            print("id: 7696 | lastupdate: 1703383549007")
        
        elif this.flag == "avgdelay":
            results = this.avgDelay(readFromFile(test_folder + "avgdelay.json"))
            print("RECEIVED:")
            results.show()

            print("EXPECTED:")
            # As in mostdelayed3min, only 3 delays should be valid (in the last 3 minutes), but because I count all, I also add 1 valid 0.0 delay.
            # Valid delays are 50,49,48, 0 respectively - therefore the average should be 36.75.
            # There is also 1 with delay 100, but its inactive, so I dont care
            print("average_delay: 36.75")
        
        elif this.flag == "avganntime":
            results = this.avgAnnTime(readFromFile(test_folder + "avganntime.json"))
            print("RECEIVED:")
            results.show()

            print("EXPECTED:")
            # I have only 8 reports - 4 unique repeated twice with a difference of 10 000. That will be my average time between announcements.
            print("Average_time_between_reports: 10000.0")

    async def performOperation(this):
        if this.test:
            this.testData()
        else:
            this.downloadData()

async def run(args):

    if (args.mode == None):
        print("You must choose a mode using the --mode parameter! Refer to README!")
        exit(1)

    a = dataDownloader(host=args.host, port=args.port, test=args.test, flag=args.mode)
    asyncio.create_task(a.performOperation())

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", action="store", default="9999", help="port where redirected websocket is", type=int)
    parser.add_argument("-s", "--host", action="store", default="localhost", help="host where redirected websocket is")
    parser.add_argument("-t", "--test", action="store_true", help="whether to launch tests or not")
    parser.add_argument("-m", "--mode", help="operation to perform (refer to README)", choices=["north", "trains", "mostdelayed", "delayed3min", "avgdelay", "avganntime"])
    args = parser.parse_args()

    asyncio.run(run(args))
