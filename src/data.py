
class data:
    """
    Temporary holds collected data until written to the database.
    """
    def __init__(self):
        self.url_map = {}
        self.hashtag_map = {}
        self.tweet_buffer = []

    # Resets are used to clear the temporary data after they have been written
    # to the database
    def reset_urls(self):
        self.url_map = {}

    def reset_hashtags(self):
        self.hashtag_map = {}

    def reset_tweets(self):
        self.tweet_buffer = []