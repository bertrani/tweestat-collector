from collections import Counter


class data:
    """
    Temporary holds collected data until written to the database.
    """
    def __init__(self):
        self.url_counter = Counter()
        self.hashtag_counter = Counter()
        self.source_counter = Counter()
        self.lang_counter = Counter()
        self.usr_lang_counter = Counter()
        self.tweet_buffer = []
        self.counter = 0

    # Resets are used to clear the temporary data after they have been written
    # to the database
    def reset_url(self):
        self.url_counter = Counter()

    def reset_hashtag(self):
        self.hashtag_counter = Counter()

    def reset_tweets(self):
        self.tweet_buffer = []

    def reset_lang(self):
        self.lang_counter = Counter()

    def reset_source(self):
        self.source_counter = Counter()

    def reset_usr_lang(self):
        self.usr_lang_counter = Counter()

    def count(self):
        self.counter += 1