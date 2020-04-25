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
sqlDF = spark.sql(" SELECT substring(user.screen_name,0,10) as Users,max(user.followers_count) as Followers FROM BtsCovSpo WHERE text like '%coronavirus%''%covid%' group by Users order by Followers desc limit 5")
pd = sqlDF.toPandas()
pd.to_csv('output4.csv', index=False)

def plot4():
    #plt.title('Top5 most Followed Users related to Covid Tweets')
   # sns.boxenplot(y="Followers", x="Users", data=pd)
    pd.plot.scatter(x="Followers", y="Users", title="Top5 most Followed Users related to Covid Tweets")
    bytes_image = io.BytesIO()
    # plt.savefig('foo.png')
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    return bytes_image
