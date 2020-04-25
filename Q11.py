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
sqlDF = spark.sql("SELECT user.location,count(text) as Total_count FROM BtsCovSpo WHERE place.country='United States' AND user.location is not null GROUP BY user.location ORDER BY Total_count DESC LIMIT 15")
pd = sqlDF.toPandas()
pd.to_csv('output11.csv', index=False)


def plot11():
    #plt.title('15 states with their Total_Tweets Count')
    #sns.boxenplot(y="location", x="Total_count", data=pd)
    pd.plot.scatter(x="Total_count", y="location", title="15 states with their Total_Tweets Count")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
