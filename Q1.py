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
sqlDF = spark.sql("SELECT substring(user.created_at,5,3) as Month, count(user.id) as Total_Tweets_per_Month from BtsCovSpo where user.created_at <> 'null' group by month")
pd = sqlDF.toPandas()
pd.to_csv('output1.csv', index=False)

def plot1():
    pd.plot.pie(y='Total_Tweets_per_Month',
                labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Nul'],
                figsize=(7, 7))



    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
