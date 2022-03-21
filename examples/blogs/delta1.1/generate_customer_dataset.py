#From within the pyspark shell run the following code
#Command to launch pyspark shell with delta: [Uncomment below]
#pyspark --packages io.delta:delta-core_2.12:1.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

import pandas as pd
from faker import Factory
import pandas as pd
import random
from faker import Faker
fake = Faker()

#Create a dataset with 100000 rows and 11 columns
current_cdemo = ["c1", "c2", "c3", "c4", "c5"]
current_hdemo = ["h1", "h2", "h3", "h4", "h5"]
salutations = ["Mr.", "Mrs.", "Ms", "Dr", "Prof", "None"]
df1 = pd.DataFrame(columns=("customer_id", "customer_sk", "current_cdemo", "current_hdemo", "first_shipto_date", "first_sales_date", "first_name", "last_name", "preferred_cust_flag", "email_address", "last_review_date"))

for i in range(200000):
  userRecord = [fake.uuid4(), \
                fake.sha256(), \
                fake.words(1, current_cdemo, True)[0], \
                fake.words(1, current_hdemo, True)[0], \
                fake.date(), \
                fake.date(), \
                fake.first_name(), \
                fake.last_name(), \
                fake.words(1, ["Y", "N"], True)[0], \
                fake.email(), \
                fake.date()] 
  df1.loc[i] = [item for item in userRecord]

customersDF = spark.createDataFrame(df1)
customersDF.write.partitionBy('first_sales_date').format('delta').save('/temp/data/customers')


#Create a dataset with 10000 rows and 11 columns for performing merge operation on the main delta lake: customer_records
current_cdemo = ["c1", "c2", "c3", "c4", "c5"]
current_hdemo = ["h1", "h2", "h3", "h4", "h5"]
salutations = ["Mr.", "Mrs.", "Ms", "Dr", "Prof", "None"]
df2 = pd.DataFrame(columns=("customer_id", "customer_sk", "current_cdemo", "current_hdemo", "first_shipto_date", "first_sales_date", "first_name", "last_name", "preferred_cust_flag", "email_address", "last_review_date"))

for i in range(40000):
  userRecord = [fake.uuid4(), \
                fake.sha256(), \
                fake.words(1, current_cdemo, True)[0], \
                fake.words(1, current_hdemo, True)[0], \
                fake.date(), \
                fake.date(), \
                fake.first_name(), \
                fake.last_name(), \
                fake.words(1, ["Y", "N"], True)[0], \
                fake.email(), \
                fake.date()] 
  df2.loc[i] = [item for item in userRecord]

customers_mergeDF = spark.createDataFrame(df2)
customers_mergeDF.write.partitionBy('first_sales_date').format('delta').save('/temp/data/customers_merge')