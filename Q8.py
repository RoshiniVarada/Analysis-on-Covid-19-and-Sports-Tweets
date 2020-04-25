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
sqlDF = spark.sql("SELECT user.name, max(user.followers_count) as Followers FROM BtsCovSpo WHERE text like '%sports%' group by user.name order by Followers desc limit 5")
pd = sqlDF.toPandas()
pd.to_csv('output8.csv', index=False)


def plot8():
    pd.plot.line(x="name", y="Followers", title="Top5 most Followed Users related to Sports Tweets")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
