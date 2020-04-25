from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "2219941182-hJEd5re1y7lbZmVlyZySZvVsJf88fP6um3SsC3r"
access_token_secret = "BntHym97rzCisKS3BFXqrBgQbgokklZEBcqHXixGJQtX8"
consumer_key = "187ztf3hxmT3Nm3YonFzcAvEB"
consumer_secret = "hTqPaSjNXw21GXmPCey6CZBCZRoO1EbTkbVO4zMv77kN8Ikq0P"



#This is a basic listener that just prints received tweets to stdout.
class TwitterStreamer():
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token,access_token_secret)
        stream = Stream(auth, listener)

        
        stream.filter(track=hash_tag_list)



class StdOutListener(StreamListener):
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a+') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)



 
if __name__ == '__main__':
 

    hash_tag_list = ['Baseball', 'Cricket', 'Football', 'Tennis', 'Golf', 'WWE', 'Badminton', 'Tennis', 'Baseball',
'Hockey', 'Volleyball', 'Rugby', 'Athletics', 'Boxing', 'MotoGP ', 'Cycling', 'Swimming', 'Snooker', 'Gymnastics',
'Handball', 'Skiing', 'Hurling', 'Bowling', 'Lacrosse', 'Archery', 'Bocce', 'Broomball', 'Croquet', 'Diving', 'Fencing',
'Darts', 'Dodgeball', 'Fishing', 'Foosball', 'Kayaking', 'Kickball', 'Racquetball', 'Powerlifting', 'Shooting', 'Sailing',
'Rowing','CoronaVirus','Covid19','coronavirus','lockdown','lockdownextension','stayhome','staysafe','COVID-19','nangi','7Today','BTS','oniscoming','namgi','jungkook','taehyung','jimin','jhope','jin','suga','RM','map of the soul','bang bang con']
    fetched_tweets_filename = "tweets.json"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)
