import pyspark
import uuid
import random

"""
To generate example data:
1. Change `table_path` to desired location
2. If you don't have pyspark installed: `pip3 install pyspark`
3. Run the script: `python3 generateParquet.py`
"""

table_path = "~/connectors/examples/standalone-example-convert-to-delta/src/main/resources/external/sales"
spark = pyspark.sql.SparkSession.builder.appName("test").getOrCreate()

columns = ["year", "month", "day", "sale_id", "customer", "total_cost"]

def generate_data():
    return [(y, m, d, str(uuid.uuid4()), str(random.randrange(10000) % 26 + 65) * 3, random.random()*10000)
    for d in range(1, 29)
    for m in range(1, 13)
    for y in range(2000, 2021)]

for _ in range(3):
    spark.sparkContext.parallelize(generate_data()).toDF(columns).repartition(1).write.parquet(table_path, mode="append")
