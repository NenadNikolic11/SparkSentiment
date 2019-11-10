import findspark
import socket
import sys
import requests
import requests_oauthlib
import json


import pyspark
import random
sc = pyspark.SparkContext(appName="Pi")
print(sc)
num_samples = 100000000
def inside(p):
  x, y = random.random(), random.random()
  return x*x + y*y < 1
#count = sc.parallelize(range(0, num_samples)).filter(inside).count()
#pi = 4 * count / num_samples
#print(pi)
#sc.stop()
#
ACCESS_TOKEN = '1154504792850522118-NtdluZJXE8HAogKlj5gcjsOqjgQ1oP'
ACCESS_SECRET = 'pfqdmz698MsvP3tQmoNsOha2ibeLPV6SLM7aThQKePh4z'
CONSUMER_KEY = 'eQtBoyaA4Qpy94GGhPuTghlTP'
CONSUMER_SECRET = 'bTyIraSczhrIqGMOD9sb4inyZw7d5D0wWDSOGfDzlElHM3BQO6'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('tweet_mode', 'extended')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response
def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            if len(line)>0:
              full_tweet = json.loads(line)
              tweet_text = full_tweet['text']

              send_text = tweet_text +'\n'
              send_text = send_text.encode("utf-8")
              tcp_connection.send(send_text)
        except:
            e = sys.exc_info()
            print("Erroreee: %s" % e)
TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)

