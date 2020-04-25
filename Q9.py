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
sqlDF = spark.sql("select count(*) as count,q.text from (select case when text like '%Cricket%' then 'cricket' when text like '%football%' then 'Football' when text like '%Tennis%' then 'Tennis' when text like '%golf%' then 'Golf' when text like '%baseball%' then 'Baseball' when text like '%rugby%' then 'Rugby' when text like '%Baseball%' then 'Baseball' WHEN text like '%Badminton%' THEN 'Badminton' WHEN text like '%Hockey%' THEN 'Hockey' WHEN text like '%Volleyball%' THEN 'Volleyball'when text like '%boxing%' then 'Boxing'when text like '%cycling%' then 'Cycling'when text like '%swimming%' then 'Swimming'when text like '%Archery%' then 'Archery'when text like '%Cricket%' then 'cricket'when text like '%shooter%' then 'Shooter'when text like '%bowling%' then 'Bowling'  end as text from BtsCovSpo)q where text <> 'null' group by q.text")
pd = sqlDF.toPandas()
pd.to_csv('output9.csv', index=False)


def plot9():
   # plt.title('No. of Tweets based on Sports')
   # sns.pointplot(y="text", x="count", data=pd)
    pd.plot(kind="bar",x="text",y="count",title="No. of Tweets based on Sports")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
