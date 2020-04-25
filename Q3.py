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
sqlDF = spark.sql("SELECT count(*) as count, user.name from BtsCovSpo where user.name is not null group by user.name order by count desc limit 10")
pd = sqlDF.toPandas()
pd.to_csv('output3.csv', index=False)

def plot3():
   # plt.title("Users with most Tweets")
  #  sns.stripplot(y="name", x="count", data=pd)
    pd.plot.scatter(x="count",y="name",title="Users with most Tweets")
    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
