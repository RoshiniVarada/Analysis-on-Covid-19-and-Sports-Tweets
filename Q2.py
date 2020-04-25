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
sqlDF = spark.sql("select count(*) as Tweets, 'Covid' as Category from BtsCovSpo where text like '%covid19%' or text like '%coronavirus%' UNION select count(*) as Tweets, 'BTS' as Category from BtsCovSpo where text like '%bts%' UNION select count(*) as Tweets, 'Sports' as Category from BtsCovSpo where text like '%sports%'")
pd = sqlDF.toPandas()
pd.to_csv('output2.csv', index=False)

def plot2():
    pd.plot(kind="bar", x="Category", y="Tweets",
            title="Total Tweets based on Categories")

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
