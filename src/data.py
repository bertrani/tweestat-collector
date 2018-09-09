
class data:
    """
    Temporary holds collected data until written to the database.
    """
    def __init__(self):
        self.url_map = {}
        self.hashtag_map = {}
        self.source_map = {}
        self.lang_map = {}
        self.usr_lang_map = {}
        self.tweet_buffer = []
        self.counter = 0

    # Resets are used to clear the temporary data after they have been written
    # to the database
    def reset_url(self):
        self.url_map = {}

    def reset_hashtag(self):
        self.hashtag_map = {}

    def reset_tweets(self):
        self.source_map = {}

    def reset_lang(self):
        self.lang_map = {}

    def reset_source(self):
        self.source = {}

    def reset_usr_lang(self):
        self.usr_lang_map = {}

    def count(self):
        self.counter += 1