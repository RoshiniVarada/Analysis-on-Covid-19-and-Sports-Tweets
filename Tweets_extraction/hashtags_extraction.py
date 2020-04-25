import json
import time


def process_file(file_name):
    time_stamp = time.strftime("%Y%m%d_%H_%M_%S")
    hash_op_file = open('hastags_PB.txt', "a+", encoding='utf-8')
    url_op_file = open('Urls_PB.txt', "a+", encoding='utf-8')
    cnt=0;
    with open(file_name, "r", encoding='utf-8') as f:
        if f.mode == "r":
            tweets = f.read().splitlines()
            try:
                for tweet in tweets[::2]:
                    cnt=cnt+1
                    content = json.loads(tweet)
                    ents= content['entities']
                    if len(ents) > 0:
                        hashs = content['entities']['hashtags']
                        if len(hashs) > 0:
                            text = hashs[0]['text']
                            hash_op_file.write(text+'\n')
                        urls = content['entities']['urls']
                        if len(urls) > 0:
                            url = urls[0]['url']
                            url_op_file.write(url+'\n')
                            print(text, url,cnt)
            except:
                pass
    hash_op_file.close()
    url_op_file.close()


process_file("tweets.json")
