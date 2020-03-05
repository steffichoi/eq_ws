from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Window, Row
import pyspark.sql.functions as f
import math


sc = SparkContext('local','example')

sqlContext = SQLContext(sc)

rdd = sqlContext.read.format('csv').options(header='true', inferschema='true').load('../../tmp/data/DataSample.csv')
df = rdd.toDF('id', 'time', 'country', 'province', 'city', 'lat', 'long')

#  1
# create an additional column counting the occurances of data from TimeSt, Longitude andd Latitude
# removing any record that has more than one occurance then cleaning up the additional column
w = Window.partitionBy(['time', 'lat', 'long'])
df = df.select('*', f.count('time').over(w).alias('dupeCount')).where("dupeCount = 1").drop('dupeCount')

# 2
# get the POI list
POI_df = sqlContext.read.format('csv').options(header='true', inferschema='true').load('../../tmp/data/POIList.csv')

# cross join dataframes to have reference to POI longitude and latitude in the same row for calculation
df_2 = df.crossJoin(POI_df.select('*'))

# belated realizing that calculating geographical distance is supposed to be done with Haversine formula
# I used the formula for distance between points: sqrt((x2-x1)^2 + (y2-y1)^2)
# Using the Haversine formula, I would have to create a UDF which takes in both geographical points and returns the distance
df_2 = df_2.withColumn('dist', f.sqrt(((df_2["lat"]-df_2[" Latitude"])**2)+((df_2["long"]-df_2["Longitude"])**2)))

# get the minimum distance for each unique request, then join with the original table to get the closest POIID
df_3 = df_2.groupby(['time', 'lat', 'long']).min('dist')
df_3 = df_3.join(df_2, (df_3['time'] == df_2['time']) & (df_3['lat'] == df_2['lat']) & (df_3['long'] == df_2['long']) & (df_3['min(dist)'] == df_2['dist'])).drop(df_2['lat']).drop(df_2['long']).drop(df_2['time'])
df_3 = df_3.drop(df_3[' Latitude']).drop(df_3['Longitude'])

# 3
# aggregation for average and standard deviation
df_5 = df_3.groupby('POIID').agg(f.avg('dist'), f.stddev('dist'))

# using max(dist) as the radius - the furthest point from the POI should be the radius to use to draw the circle
df_6 = df_3.groupby('POIID').agg(f.max('dist'), f.count('dist'))
# calculate density(req/area)
fin = df_6.withColumn('density', df_6['count(dist)']/(math.pi*(df_6['max(dist)']**2)))

# clean up on additional distance columns
df_3 = df_3.drop(df_3['min(dist)']).drop(df_3['dist'])

# write all to csv
df.write.csv('/tmp/data/filtered.csv', header='true')
df_3.write.csv('/tmp/data/POI_labeled.csv', header='true')
df_5.write.csv('/tmp/data/avg_stddev.csv', header='true')
fin.write.csv('/tmp/data/density.csv', header='true')

