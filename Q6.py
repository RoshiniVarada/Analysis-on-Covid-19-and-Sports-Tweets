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
sqlDF = spark.sql("SELECT place.country,count(*) AS count FROM BtsCovSpo where place.country <> 'null' GROUP BY place.country ORDER BY count DESC limit 10")
pd = sqlDF.toPandas()
pd.to_csv('output6.csv', index=False)


def plot6():
    pd.plot.area(x="country", y="count", title="Tweet Count from Different Countries")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
