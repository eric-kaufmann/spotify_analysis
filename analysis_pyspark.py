from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql.functions import col,avg

sc = SparkContext()
sqlContext=SQLContext(sc)
pldf = sqlContext.read.csv("./data/playlists.csv",header=True, inferSchema=True)
sdf = sqlContext.read.csv("./data/tracks_features.csv",header=True,inferSchema=True)

pldf = pldf.withColumnRenamed(' "artistname"',"artist") \
    .withColumnRenamed("user_id","user") \
    .withColumnRenamed(' "trackname"',"track") \
    .withColumnRenamed(' "playlistname"',"playlist")
sdf = sdf.drop("artist_ids", "album_id") \
    .withColumnRenamed("artists","artist") \
    .withColumnRenamed("name","track")

sdf = sdf.withColumn("artist", f.split(col("artist"), ",")) \
        .withColumn("artist", f.explode(col("artist"))) \
        .withColumn("artist",f.trim(col("artist"))) \
        .withColumn("artist", f.translate(col("artist"),"[]\"'",""))


#for testing purposes take a smaller subset
pldf = pldf.limit(1000)

merge_df = pldf.join(sdf, on = ["artist","track"],how = "left")
merge_df = merge_df.dropna()

num_tracks = merge_df.select("track").distinct().count()
num_artists = merge_df.select("artist").distinct().count()
num_rows = merge_df.count()
print(num_tracks,num_artists,num_rows)

pl_grouped = merge_df.groupBy("playlist")

# Calculate the average attribute vals for each playlist (median probably better)
pl_avg = pl_grouped.agg(avg("energy"), avg("danceability"), avg("loudness"), avg("speechiness"),avg("acousticness"), avg("instrumentalness"), avg("liveness"), avg("valence"), avg("tempo"))

# using pandas instead of .show() to reduce number of function calls and the amount of data that needs to be processed and transferred between the driver and the executors.
print(pl_avg.toPandas().head())

sdf = sdf.limit(2000)

# change of attribute vals over time (median probably better)
s_grouped = sdf.groupBy("year")
s_avg = s_grouped.agg(avg("energy"), avg("danceability"), avg("loudness"), avg("speechiness"),avg("acousticness"), avg("instrumentalness"), avg("liveness"), avg("valence"), avg("tempo"))
s_avg = s_avg.orderBy("year")
print(s_avg.toPandas().head(500))



sc.stop()

