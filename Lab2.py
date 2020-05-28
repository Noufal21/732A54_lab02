

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import col, asc
import shutil
import os
if os.path.isdir('BDASQL'):
    shutil.rmtree('BDASQL')

sc = SparkContext(appName = "exercise 1")
sqlContext = SQLContext(sc)
# This path is to the file on hdfs

temperature_file = sc.textFile("BDASQL/input/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))
tempRow = lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
tempColumns = ["station", "date", "year", "month", "time", "value",
"quality"]
tempTable = sqlContext.createDataFrame(tempRow,tempColumns)


precipitation_file = sc.textFile("BDASQL/input/precipitation-readings.csv")

precipitation_lines  = precipitation_file.map(lambda line: line.split(";"))

precipitationRow = precipitation_lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
precipitationColumns = ["station", "date", "year", "month", "time", "value",
"quality"]
precipitationTable = sqlContext.createDataFrame(precipitationRow,precipitationColumns)


ostergotland_file = sc.textFile("BDASQL/input/stations-Ostergotland.csv")

ostergotland_lines = ostergotland_file.map(lambda line : line.split(";"))
ostergotlandRow = ostergotland_lines.map(lambda p: (p[0], p[1]))
ostergotlandColumns = ["station", "stationName"]
ostergotlandTable = sqlContext.createDataFrame(ostergotlandRow,ostergotlandColumns)

maxTemp  = tempTable.where(col('year') >= 1950).where(col('year') <= 2014).groupBy('year').agg(F.max('value').alias('MaxTemp'),F.min('value').alias('MinTemp')).orderBy(['MaxTemp'],ascending=[0])
maxTemp.rdd.saveAsTextFile("BDASQL/question1")




greater_10_temperature_count  = tempTable.where(col('year') >= 1950).where(col('year') <= 2014).where(col('value') >= 10).groupBy('year','month').agg(F.count('value').alias('CountTemp')).orderBy(['CountTemp'],ascending=[0])
greater_10_temperature_count.rdd.saveAsTextFile("BDASQL/question2_1")


greater_10_temperature_mapped_count  = tempTable.where(col('year') >= 1950).where(col('year') <= 2014).where(col('value') >= 10).groupBy('year','month').agg(F.countDistinct('station').alias('CountTemp')).orderBy(['CountTemp'],ascending=[0])
greater_10_temperature_mapped_count.rdd.saveAsTextFile("BDASQL/question2_2")


station_maximum_temperature = tempTable.groupBy('station').agg(F.max('value').alias('MaxTemp')).orderBy(['MaxTemp'],ascending=[0])

station_maximum_precipitation = precipitationTable.groupBy('year','month','date','station').agg(F.max('value').alias('MaxPrecipitation')).orderBy(['MaxPrecipitation'],ascending=[0])

join_table = station_maximum_temperature.join(station_maximum_precipitation,station_maximum_temperature['station'] == station_maximum_precipitation['station'],'inner' )
avg_year_month_sation_temperature  = tempTable.where(col('year') >= 1950).where(col('year') <= 2014).groupBy('year','month','station').agg(F.avg('value').alias('Avgmontlytemperature')).orderBy(['Avgmontlytemperature'],ascending=[0])
avg_year_month_sation_temperature.rdd.saveAsTextFile("BDASQL/question3")


station_maximum_temperature = tempTable.groupBy('station').agg(F.max('value').alias('MaxTemp')).orderBy(['MaxTemp'],ascending=[0])

station_maximum_precipitation = precipitationTable.groupBy('year','month','date','station').agg(F.max('value').alias('MaxPrecipitation')).orderBy(['MaxPrecipitation'],ascending=[0])

join_table = station_maximum_temperature.join(station_maximum_precipitation,station_maximum_temperature['station'] == station_maximum_precipitation['station'],'inner' )
join_table.where(col('MaxPrecipitation') >= 10).where(col('MaxPrecipitation') <= 1000).where(col("MaxTemp") >= 25).where(col('MaxTemp') <=30)
join_table.rdd.saveAsTextFile("BDASQL/question4")


#ostergotlandTable
OT = ostergotlandTable.withColumnRenamed('station','ST')
ostergotland_precipitationTable = precipitationTable.join(OT,OT['ST']== precipitationTable['station'],'inner')
ostergotland_precipitationTable_avg = ostergotland_precipitationTable.where(col('year') >= 1993).where(col('year') <= 2016).groupBy('year','month').agg(F.avg('value').alias('Avgmontlytemperature')).orderBy(['Avgmontlytemperature'],ascending=[0])
ostergotland_precipitationTable_avg.rdd.saveAsTextFile("BDASQL/question5")
