from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

import io

spark = SparkSession \
    .builder \
    .appName("Twitter Data Analysis") \
    .getOrCreate()
df = spark.read.json("tweetsdata.json")
df.createOrReplaceTempView("BtsCovSpo")
sqlDF = spark.sql("select count(*) as Total_count, lang as Language from BtsCovSpo group by lang order by Total_count desc limit 11")
pd = sqlDF.toPandas()
pd.to_csv('output12.csv', index=False)


def plot12():
    #plt.title('Top10 Languages used by Tweets')
    #sns.pointplot(y="Total_count", x="Language",data=pd)
    pd.plot.line(x="Language", y="Total_count", title="Top10 Languages used by Tweets")
    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
