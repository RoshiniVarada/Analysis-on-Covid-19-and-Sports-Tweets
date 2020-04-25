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
sqlDF = spark.sql("SELECT user.screen_name,text,retweeted_status.retweet_count FROM BtsCovSpo ORDER BY retweeted_status.retweet_count DESC LIMIT 10")
pd = sqlDF.toPandas()
pd.to_csv('output13.csv', index=False)


def plot13():
    pd.plot(kind="bar", y="retweet_count", x="screen_name",
            title="Top10 Users with the most Retweets")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
