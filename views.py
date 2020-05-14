from flask import Flask, send_file, render_template
import seaborn as sns
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas
from io import BytesIO

app = Flask(__name__)


@app.route("/")  # at the end point /
def hello1():  # call method hello
    return render_template('index.html')


@app.route("/query/<id>", methods=['GET'])
def hello(id):
    if id == "1":
        query1 = spark.sql(
            "SELECT substring(user.created_at,5,3) as Month, count(user.id) as Total_Tweets_per_Month from BtsCovSpo where user.created_at <> 'null' group by month")
        pd1 = query1.toPandas()
        pd1.to_csv('output1.csv', index=False)
        pd1.plot.pie(y='Total_Tweets_per_Month',
                     labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Nul'],
                     figsize=(7, 7))
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "2":
        query2 = spark.sql(
            "select count() as Tweets, 'Covid' as Category from BtsCovSpo where text like '%covid19%'  or text like '%coronavirus%' UNION select count() as Tweets, 'BTS' as Category from BtsCovSpo where text like '%bts%' UNION select count(*) as Tweets, 'Sports' as Category from BtsCovSpo where text like '%sports%'")
        pd2 = query2.toPandas()
        pd2.to_csv('output2.csv', index=False)
        pd2.plot(kind="bar", x="Category", y="Tweets",
                 title="Total Tweets based on Categories")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "3":
        query3 = spark.sql(
            "SELECT count(*) as count, user.name from BtsCovSpo where user.name is not null group by user.name order by count desc limit 10")
        pd3 = query3.toPandas()
        pd3.to_csv('output3.csv', index=False)
        # plt.title("Users with most Tweets")
        #  sns.stripplot(y="name", x="count", data=pd)
        pd3.plot.scatter(x="count", y="name", title="Users with most Tweets")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')

    if id == "4":
        query4 = spark.sql(
            "SELECT substring(user.screen_name,0,10) as Users,max(user.followers_count) as Followers FROM BtsCovSpo WHERE text like '%coronavirus%''%covid%' group by Users order by Followers desc limit 5")
        pd4 = query4.toPandas()
        pd4.to_csv('output4.csv', index=False)
        # plt.title('Top5 most Followed Users related to Covid Tweets')
        # sns.boxenplot(y="Followers", x="Users", data=pd)
        pd4.plot.scatter(x="Followers", y="Users", title="Top5 most Followed Users related to Covid Tweets")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "5":
        query5 = spark.sql(
            "SELECT substring(user.created_at,27,4) as year,count(*) as Total from BtsCovSpo where user.created_at is not null group by substring(user.created_at,27,4) order by year desc")
        pd5 = query5.toPandas()
        pd5.to_csv('output5.csv', index=False)
        pd5.plot(kind="barh", x="year", y="Total",
                 title="Users created per Year")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "6":
        query6 = spark.sql(
            "SELECT place.country,count(*) AS count FROM BtsCovSpo where place.country <> 'null' GROUP BY place.country ORDER BY count DESC limit 10")
        pd6 = query6.toPandas()
        pd6.to_csv('output6.csv', index=False)
        pd6.plot.area(x="country", y="count", title="Tweet Count from Different Countries")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "7":
        query7 = spark.sql(
            "SELECT substring(quoted_status.created_at,1,3) as Days,count(text) as Total_Tweets_per_Day FROM BtsCovSpo where quoted_status.created_at <> 'null' GROUP BY Days")
        pd7 = query7.toPandas()
        pd7.to_csv('output7.csv', index=False)
        pd7.plot.pie(y='Total_Tweets_per_Day',
                     labels=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                     figsize=(5, 5))
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "8":
        query8 = spark.sql(
            "SELECT user.name, max(user.followers_count) as Followers FROM BtsCovSpo WHERE text like '%sports%' group by user.name order by Followers desc limit 5")
        pd8 = query8.toPandas()
        pd8.to_csv('output8.csv', index=False)
        pd8.plot.line(x="name", y="Followers", title="Top5 most Followed Users related to Sports Tweets")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "9":
        query9 = spark.sql(
            "select count(*) as count,q.text from (select case when text like '%Cricket%' then 'cricket' when text like '%football%' then 'Football' when text like '%Tennis%' then 'Tennis' when text like '%golf%' then 'Golf' when text like '%baseball%' then 'Baseball' when text like '%rugby%' then 'Rugby' when text like '%Baseball%' then 'Baseball' WHEN text like '%Badminton%' THEN 'Badminton' WHEN text like '%Hockey%' THEN 'Hockey' WHEN text like '%Volleyball%' THEN 'Volleyball'when text like '%boxing%' then 'Boxing'when text like '%cycling%' then 'Cycling'when text like '%swimming%' then 'Swimming'when text like '%Archery%' then 'Archery'when text like '%Cricket%' then 'cricket'when text like '%shooter%' then 'Shooter'when text like '%bowling%' then 'Bowling'  end as text from BtsCovSpo)q where text <> 'null' group by q.text")
        pd9 = query9.toPandas()
        pd9.to_csv('output9.csv', index=False)
        pd9.plot(kind="bar", x="text", y="count", title="No. of Tweets based on Sports")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "10":
        query10 = spark.sql(
            "SELECT user.verified,user.screen_name,max(user.followers_count) as followers_count FROM BtsCovSpo WHERE user.verified = true GROUP BY user.verified, user.screen_name LIMIT 10")
        pd10 = query10.toPandas()
        pd10.to_csv('output10.csv', index=False)
        pd10.plot.scatter(x="followers_count", y="screen_name", title="verified users with most followers")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "11":
        query11 = spark.sql(
            "SELECT user.location,count(text) as Total_count FROM BtsCovSpo WHERE place.country='United States' AND user.location is not null GROUP BY user.location ORDER BY Total_count DESC LIMIT 15")
        pd11 = query11.toPandas()
        pd11.to_csv('output11.csv', index=False)
        pd11.plot.scatter(x="Total_count", y="location", title="15 states with their Total_Tweets Count")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "12":
        query12 = spark.sql(
            "select count(*) as Total_count, lang as Language from BtsCovSpo group by lang order by Total_count desc limit 11")
        pd12 = query12.toPandas()
        pd12.to_csv('output12.csv', index=False)
        # plt.title('Top10 Languages used by Tweets')
        # sns.pointplot(y="Total_count", x="Language",data=pd)
        pd12.plot.line(x="Language", y="Total_count", title="Top10 Languages used by Tweets")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
        if id == "11":
            query13 = spark.sql(
                "SELECT user.screen_name,text,retweeted_status.retweet_count FROM BtsCovSpo ORDER BY retweeted_status.retweet_count DESC LIMIT 10")
            pd13 = query13.toPandas()
            pd13.to_csv('output13.csv', index=False)
            pd13.plot(kind="bar", y="retweet_count", x="screen_name", title="Top10 Users with the most Retweets")
            img = BytesIO()
            plt.savefig(img)
            img.seek(0)
            return send_file(img, mimetype='image/png')
        if id == "12":
            query14 = spark.sql(
                "SELECT substring(user.screen_name,0,10) as User,max(user.followers_count) as Followers FROM BtsCovSpo WHERE text like '%bts%' group by User order by Followers desc limit 5")
            pd14 = query14.toPandas()
            pd14.to_csv('output14.csv', index=False)
            pd14.plot.area(x="User", y="Followers", title="Top5 most Followed Users related to BTS Tweets")
            img = BytesIO()
            plt.savefig(img)
            img.seek(0)
            return send_file(img, mimetype='image/png')


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Phase 2 querying and plotting").getOrCreate()
    sc = spark.sparkContext
    df = spark.read.json("tweetsdata.json")
    df.createOrReplaceTempView("BtsCovSpo")
    app.run(debug=True)
