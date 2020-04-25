from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import io

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()
df = spark.read.json("tweetsdata.json")
df.createOrReplaceTempView("BtsCovSpo")
sqlDF = spark.sql("SELECT substring(quoted_status.created_at,1,3) as Days,count(text) as Total_Tweets_per_Day FROM BtsCovSpo where quoted_status.created_at <> 'null' GROUP BY Days")
pd = sqlDF.toPandas()
pd.to_csv('output7.csv', index=False)


def plot7():
    pd.plot.pie(y='Total_Tweets_per_Day',
                labels=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                figsize=(5, 5))

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
