import time
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from influxdb import InfluxDBClient
import json

access_token = "971353729369853952-nd1J0LwfQ2eaUbfGycZGIuvlPjb79JF"
access_token_secret = "iI7gDWynLj7hlS4MtWI2kdFSTZIwymtsSszhdR13JxMRS"
consumer_key = "JxZR7y7x7y6iR7GDO8O6lm6dM"
consumer_secret = "XuevFaX5ztvOKYOAibaZxjav3YypAjJ1zer4Twaz9jM4Z6eRgn"

hashtag = [{"measurement": "hashtag",
            "tags": {
                "hash": ""
            },
            "fields": {
                "count":""
            }}]

tweet = [{"measurement": "tweet",
          "tags": {
              # NOMINAL
              "language":"",
              "source":"",
              "usr_language":""
          },
          "fields": {
              "no_hashtags":"",
              "no_urls":"",
              "no_usr_follower":"",
              "no_usr_friend":"",
              "no_usr_statuses":"",
              "no_usr_favourites":"",
              #ORDINAL
              "is_sensitive":"",
              "is_quote":"",
              "is_retweeted":"",
              "has_coordinates":"",
              "has_place":"",
              "has_hash":"",
              "has_url":"",
              "has_poll":"",
              "usr_is_verified":"",
              #NOT YET INCLUDED IN THESIS:
              "no_words":"",
              "counter":1
          }}]

class StdOutListener(StreamListener):
    def __init__(self):
        super().__init__()
        self.client = InfluxDBClient('localhost', 8086, 'admin', 'targa123', 'tweestat_test')

    def on_data(self, data):
        if data.startswith("{\"created_at"):
            json_data = json.loads(data)

            self.build_tweet(json_data)
            self.client.write_points(tweet)

            #for tag in json_data["entities"]["hashtags"]:
            #    hashtag[0]["tags"]["hash"] = tag["text"]
            #    self.client.write_points(hashtag)



    def on_error(self, status):
        print(status)

    def build_tweet(self, json_data):
        self.entities_count_reader(json_data, 'hashtags')
        self.entities_count_reader(json_data, 'urls')
        self.user_reader(json_data, 'followers_count')
        self.user_reader(json_data, 'friends_count')
        self.user_reader(json_data, 'statuses_count')
        self.user_reader(json_data, 'favourites_count')
        #self.boolean_reader(json_data, 'possibly_sensitive')
        #self.boolean_reader(json_data, 'is_quote_status')
        self.exists_reader(json_data, 'retweeted_status')
        self.null_reader(json_data, 'coordinates')
        self.null_reader(json_data, 'place')

        try:
            tweet[0]["tags"]["usr_language"] = json_data["user"]["lang"]
        except KeyError:
            print('usr_language error')

        try:
            tweet[0]["tags"]["tweet_language"] = json_data["lang"]
        except KeyError:
            print('tweet_language_error')

        try:
            source = json_data["source"]
            tweet[0]["tags"]["source"] = source[source.index(">")+1:source.index("<", source.index(">") + 1)]
        except KeyError:
            print('source_error')

    def entities_count_reader(self, json_data, field):
        try:
            tweet[0]["fields"]["no_" + field] = len(json_data["entities"][field])
        except KeyError:
            tweet[0]["fields"]["no_" + field] = 0
            print("Count reader error on: " + field)

    def user_reader(self, json_data, field):
        try:
            tweet[0]["fields"]["usr_" + field] = json_data["user"][field]
        except KeyError:
            tweet[0]["fields"]["usr_" + field] = 0
            print('User reader error on: ' + field)

    def null_reader(self, json_data, field):
        try:
            if json_data[field]:
                tweet[0]["fields"]['has_' + field] = 1
            else:
                tweet[0]["fields"]['has_' + field] = 0
        except KeyError:
            tweet[0]["fields"]['has_' + field] = 0
            print('Null reader error on: ' + field)

    def boolean_reader(self, json_data, field):
        try:
            if json_data[field] == 'true':
                tweet[0]["fields"][field] = 1
            else:
                tweet[0]["fields"][field] = 0
        except KeyError:
            print('Boolean reader error on: '+ field)
            tweet[0]["fields"][field] = 0

    def exists_reader(self, json_data, field):
        try:
            if json_data[field]:
                tweet[0]["fields"][field] = 1
            else:
                tweet[0]["fields"][field] = 0
        except KeyError:
            tweet[0]["fields"][field] = 0
            # print("Exists reader error on: " + field)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    print("STARTED ...")
    connection = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, connection)
    stream.sample(stall_warnings=True)
