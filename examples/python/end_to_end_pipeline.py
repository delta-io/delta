import os
# spark
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#download the nyc city data from kaggle website(https://www.kaggle.com/c/new-york-city-taxi-fare-prediction) on the local or the hdfs filesystem
spark=SparkSession.builder.appName("ML workloads").getOrCreate()
#Add the delta lake jar file
spark.sparkContext.addPyFile("file:///opt/external_lib/delta-core_2.11-0.4.0.jar")
from delta.tables import *

#Constants
MIN_LONG = -74.3
MAX_LONG = -73.0
MIN_LAT = 40.6
MAX_LAT = 41.7
MAX_PASSENGER = 10 
MIN_FARE = 0.0 
data=spark.read.format("csv").option("header","true") \
     .option("inferSchema","true") \
     .load("./data/train.csv")
(train_data,test_data)=data.randomSplit([0.7, 0.3])
train_data.write.format("delta").mode("overwrite").save("./data/delta/train_delta")
test_data.write.format("delta").mode("overwrite").save("./data/delta/test_delta")

deltaTable=DeltaTable.forPath(spark,"./data/delta/train_delta")
deltaTable.delete("pickup_longitude<'MIN_LONG'")
deltaTable.delete("pickup_longitude>'MAX_LONG'")
deltaTable.delete("dropoff_longitude<'MIN_LONG'")
deltaTable.delete("dropoff_longitude>'MIN_LONG'")
deltaTable.delete("pickup_latitude<'MIN_LAT'")
deltaTable.delete("pickup_latitude>'MAX_LAT'")
deltaTable.delete("dropoff_latitude<'MIN_LAT'")
deltaTable.delete("dropoff_latitude>'MIN_LAT'")
deltaTable.delete("passenger_count>'MAX_PASSENGER'")
deltaTable.delete("fare_amount < 'MIN_FARE'")
deltaTable.delete("(dropoff_longitude -pickup_longitude)>'5'")
deltaTable.delete("(dropoff_longitude -pickup_longitude)<'-5'")
deltaTable.delete("(dropoff_latitude - pickup_latitude)>'5'")
deltaTable.delete("(dropoff_latitude - pickup_latitude)<'-5'")

train_delta=spark.read.format("delta").load("./data/delta/train_delta")
print("Old Size:")
print(train_data.count())
print("After Clean Data Size:")
print(train_delta.count())
#change schema
train_delta=train_delta.withColumn('fare_amount', regexp_replace('fare_amount', '%', '').cast('float')).withColumn('pickup_longitude', regexp_replace('pickup_longitude', '%', '').cast('float')).withColumn('pickup_latitude', regexp_replace('pickup_latitude', '%', '').cast('float')).withColumn('dropoff_longitude', regexp_replace('dropoff_longitude', '%', '').cast('float')).withColumn('dropoff_latitude', regexp_replace('dropoff_latitude', '%', '').cast('float')).withColumn('passenger_count', regexp_replace('passenger_count', '%', '').cast('float'))
print("Before Schema:")
train_data
print("After Schema:")
train_delta
train_data_delta=train_delta.select("pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count","fare_amount")
trainingData=train_data_delta.rdd.map(lambda x:(Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
trainingData.show()
# Train a RandomForest model.
rf = RandomForestRegressor(labelCol="label", featuresCol="features")
model = rf.fit(trainingData)
test_delta=spark.read.format("delta").load("./data/delta/test_delta")
print("Test Data Size:")
print(test_delta.count())
test_data_delta=test_delta.select("pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count","fare_amount")
print("Test Data Show:")
test_data_delta.show()
testData=test_data_delta.rdd.map(lambda x:(Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(30)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
labelCol="label", predictionCol="prediction", metricName="rmse")
print(" *** Root Mean Squared Error (RMSE) on test data = %g" % rmse)

