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
sqlDF = spark.sql("SELECT user.verified,user.screen_name,max(user.followers_count) as followers_count FROM BtsCovSpo WHERE user.verified = true GROUP BY user.verified, user.screen_name LIMIT 10")
pd = sqlDF.toPandas()
pd.to_csv('output10.csv', index=False)


def plot10():
    pd.plot.scatter(x="followers_count", y="screen_name", title="verified users with most followers")
    #plt.title('Verified User Accounts with most Followers')
    #sns.stripplot(y="screen_name", x="followers_count", data=pd)

    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
