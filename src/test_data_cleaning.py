import unittest
import data_cleaning
import ray

class TestTwitterPipelining(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestTwitterPipelining, cls).setUpClass()
        cls.NUM_OF_COPIES = 100
        ray.init()

    def setUp(self):
        self.NUM_OF_COPIES = 100

    def test_pipelining_without_ray(self):
        # Read json file
        tweets = data_cleaning.read_tweet_json_file()
        # increase num of tweets
        tweets = tweets * self.NUM_OF_COPIES
        
        result = data_cleaning.twitter_pipelining(tweets)
        for r in result:
            if r != {}:
                self.assertTrue(r['lang'] == 'en')
                self.assertTrue('media' not in r['entities'])
                self.assertTrue('urls' not in r['entities'])
                self.assertTrue('http' not in r['text'])
                self.assertTrue(data_cleaning.text_has_emoji(r['text']) is False)
    
    def test_pipelining_with_ray(self):
        # Read json file
        tweets = data_cleaning.read_tweet_json_file()
        # increase num of tweets
        tweets = tweets * self.NUM_OF_COPIES
        
        result = data_cleaning.twitter_pipelining_with_ray(tweets)
        for r in result:
            if r != {}:
                self.assertTrue(r['lang'] == 'en')
                self.assertTrue('media' not in r['entities'])
                self.assertTrue('urls' not in r['entities'])
                self.assertTrue('http' not in r['text'])
                self.assertTrue(data_cleaning.text_has_emoji(r['text']) is False)
    
    def test_performance(self):
        print("\nPerformance Test started.")
        data_cleaning.performance_test()

if __name__ == '__main__':
    unittest.main()