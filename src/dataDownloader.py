from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from sys import argv
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, first, window, collect_list, avg, max_by, element_at, from_json, col
import time
import asyncio
import argparse

from redirectToLocalhost import Redirecter

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
    
    def __init__(this, port, flag, test=False):
        this.port = port
        this.test = test
        this.flag = flag

    def getData(this):
        # todo: when test=true, load data from file instead of stream
        if (this.test):
            this.loadDataFromFiles()
        # Else just downlaod it from source
        else:
            this.downloadData()

        return this.data
    
    def loadDataFromFiles(this):
        return
    
    # Method to filter only the latest results from the data found using "complete"
    def getLatestResults(this, stream, tmpCol):
        
        # group by the attributes I want by ID, then aggregate the maximum update value AND based on the lastudpate value find correspoding searched col
        most_recent = stream.groupBy(["attributes.id"]).agg(max("attributes.lastupdate").alias("lastupdate"), max_by(tmpCol, "attributes.lastupdate").alias(tmpCol.replace("attributes.", "")))

        return most_recent
    
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

    def parseNorth(this, stream):
        # North is 360, with tolerance 45 that is -> <(315 - 45) % 360, 360> or <0, (360 + 45) % 360>
        onlyNorth = stream.filter(col("attributes.bearing").between(0, 45) | col("attributes.bearing").between(315, 360))

        # Only select ID, bearing and lastupdate columns. I need to run groupby so that I can run aggregation, which is only done because I can't print out
        # results otherwise. Another approach would be to save the output into a file and then use another spark app to read from it,
        # but I'm not sure whether I should do that or this.
        onlySomeCols = onlyNorth.groupby(["attributes.id", "attributes.bearing", "attributes.lastupdate"]).agg(first("attributes.delay"))

        # Drop the unnecessary aggregation
        onlySomeCols = onlySomeCols.drop("first(attributes.delay)")

        if this.test:
            onlySomeCols.show()
        else:
            # Output the data every 10s
            # Update means only show those that changed (which is everything because I am using and since I am using lastupdate, I am overwriting them
            # Complete would mean show everything it ever recorded which would show thousands of results (good for actual aggregations)
            query = this.writeOutput(onlySomeCols, "update")
            query.awaitTermination()

    def parseTrains(this, stream):

        # Find columns with vtype == 5, which means its a train
        trains = stream.filter(col("attributes.vtype") == 5)

        latest = this.getLatestResults(trains, "attributes.laststopid")

        if this.test:
            latest.show()
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(latest, "complete")

            query.awaitTermination()

    def parseDelayed(this, stream):
        
        # Latest delay since the app launch
        latest = this.getLatestResults(stream, "attributes.delay")

        # Order by delay, descending, filter where there is any and show only the top 5 most delayed
        latest = latest.orderBy("delay", ascending=False).filter(col("delay") > 0).limit(5)

        if this.test:
            latest.show()
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(latest, "complete")

            query.awaitTermination()

    def parseMostDelayedInLastThreeMinutes(this, stream):
        
        # I'd like to convert it to timestamp to use with a window....
        #stream = stream.withColumn("attributes.lastupdate", expr("attributes.lastupdate / 1000"))
        #stream = stream.withColumn("attributes.lastupdate", col("attributes.lastupdate").cast(TimestampType()))

        # Ghetto solution, seems to be working...
        stream = stream.filter(col("attributes.lastupdate") > (int(time.time()*1000)-60*1000*3)) # 60 * 1000 is 1 minute times 3 is 3 minutes
        stream = stream.filter(col("delay") > 0) # I only want the delayed
        latest = stream.groupBy("attributes.id").agg(max("attributes.lastupdate").alias("lastupdate"))

        # I'D LIKE TO USE A WINDOW GROUPING HERE BUT I SIMPLY DON'T KNOW HOW. PYSPARK IS TERRIBLE. I DON'T WANT TO USE IT EVER AGAIN. 4 HOURS SPENT ON THIS.
        
        #stream = stream.groupBy(window("attributes.lastupdate", "3 minutes"), "attributes.id").agg(first("attributes.bearing").alias("bearing")).drop("bearing")
        # Order by last update time descending
        #latest = stream.orderBy("window", ascending=False).limit(5)

        if this.test:
            latest.show()
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(latest, "complete")

            query.awaitTermination()

    def avgDelay(this, stream):

        # Ghetto solution, seems to be working... As in parseMostDelayedInLastThreeMinutes(), I'd like to use a window here... don't know how...
        # Latest 3 minutes... relies heavily on the data having synchronized timestamp just as the PC it runs on has
        # Maybe solve this by using windows from MY timestamps I can add to it?
        
        if this.test:
            max_time = int(stream.agg(max("attributes.lastupdate").alias("maxtime")).first()[0]) # doesn't work with streams... nothing does
            stream = stream.filter(col("attributes.lastupdate") > (max_time - 60*1000*3))
        
        else:
            stream = stream.filter(col("attributes.lastupdate") > (int(time.time()*1000)-60*1000*3))
        
        # calculate overall delay
        most_recent = this.getLatestResults(stream, "attributes.delay")
        overall_avg_delay = most_recent.agg(avg("delay").alias("average_delay"))

        if this.test:
            overall_avg_delay.show()
        else:
            # Complete output, ie. output everything you have, but because I filtered it, it shows only the latest data
            query = this.writeOutput(overall_avg_delay, "complete")

            query.awaitTermination()

    def avgAnnTime(this, stream):

        # Grouped by IDs and for each ID corresponding last updates in a list and ordered by latest values and get only 10 latest
        most_recent = stream.groupBy("attributes.id").agg(collect_list("attributes.lastupdate").alias("lastupdate")).orderBy(element_at(col("lastupdate"), -1)).limit(10) # Should I do this?

        # Find only the 10 most recent updates, very crude way but I don't know any better
        #only10 = most_recent.select("id", element_at(col("lastupdate"), -1), element_at(col("lastupdate"), -2), 
        #                            element_at(col("lastupdate"), -3), element_at(col("lastupdate"), -4), 
        #                            element_at(col("lastupdate"), -5), element_at(col("lastupdate"), -6),
        #                           element_at(col("lastupdate"), -7), element_at(col("lastupdate"), -8),
        #                            element_at(col("lastupdate"), -9), element_at(col("lastupdate"), -10))
        #avg = only10.withColumn("avg_for_each_id", sum(only10[x] for x in only10.columns if x[-1] == ")"))

        # Find 2 most recent updates and calculate average wait time as newer - older
        dif = most_recent.withColumn("difference", (element_at(col("lastupdate"), -1) - element_at(col("lastupdate"), -2)))

        # Find 10 most recent updates for each lastUpdate and calculate differences as (newer - older + (newerer - oldered)) / n
        #dif = most_recent.withColumn("difference", sum([element_at(col("lastupdate"), -x) - element_at(col("lastupdate"), -x-1) for x in range(1, 11)])/10) # doesnt work anyway
        
        # Calcualte average difference
        overall_avg = dif.agg(avg("difference").alias("Average time between reports"))

        if this.test:
            overall_avg.show()
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
        stream = spark.readStream.format("socket").option("host", "localhost").option("port", this.port).load()

        # Parse the data as JSON using a given schema
        parsed_stream = stream.select(from_json(col("value"), this.json_schema).alias("json_data")).select("json_data.attributes")
        #stream.coalesce(1).writeStream.outputMode("append").format("json").option("path", "./results").option("checkpointLocation", "./checkpoint").start()
        
        # Filter out those that are inactive (isInactive == false)
        parsed_stream = parsed_stream.filter(col("attributes.isinactive") == "false")

        if this.flag == "north": # Vehicles heading north
            this.parseNorth(parsed_stream)
        
        elif this.flag == "trains":
            this.parseTrains(parsed_stream)
        
        elif this.flag == "mostdelayed":
            this.parseDelayed(parsed_stream)

        elif this.flag == "mostdelayed3min":
            this.parseMostDelayedInLastThreeMinutes(parsed_stream)
        
        elif this.flag == "avgdelay":
            this.avgDelay(parsed_stream)
        
        elif this.flag == "avganntime":
            this.avgAnnTime(parsed_stream)

    def testData(this):
        spark = SparkSession.builder.appName("PDI Project websocket").getOrCreate()
        
        test_folder = "./tests/"
        json_df = spark.read.json(test_folder + "default.json")
        
        # Select only the interesting stuff
        parsed_stream = json_df.select(from_json(col("value"), this.json_schema).alias("json_data")).select("json_data.attributes")

        # Filter out those that are inactive (isInactive == false)
        parsed_stream = parsed_stream.filter(col("attributes.isinactive") == "false")

        if this.flag == "north": # Vehicles heading north
            this.parseNorth(parsed_stream)
        
        elif this.flag == "trains":
            this.parseTrains(parsed_stream)
        
        elif this.flag == "mostdelayed":
            this.parseDelayed(parsed_stream)

        elif this.flag == "mostdelayed3min":
            this.parseMostDelayedInLastThreeMinutes(parsed_stream)
        
        elif this.flag == "avgdelay":
            this.avgDelay(parsed_stream)
        
        elif this.flag == "avganntime":
            this.avgAnnTime(parsed_stream)

    async def performOperation(this):
        if this.test:
            this.testData()
        else:
            this.downloadData()

async def run(args):

    if (args.mode == None):
        print("You must choose a mode using the --mode parameter! Refer to README!")
        exit(1)

    a = dataDownloader(port=args.port, test=args.test, flag=args.mode)
    asyncio.create_task(a.performOperation())

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", action="store", default="9999", help="port where redirected websocket is", type=int)
    parser.add_argument("-t", "--test", action="store_true", help="whether to launch tests or not")
    parser.add_argument("-m", "--mode", help="operation to perform (refer to README)", choices=["north", "trains", "mostdelayed", "mostdelayed3min", "avgdelay", "avganntime"])
    args = parser.parse_args()

    asyncio.run(run(args))
