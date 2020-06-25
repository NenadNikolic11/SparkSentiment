# SparkSentiment on Twitter

Project for connecting PySpark to Twitter API and then extract sentiment using Recurent NN.

client.py
This file is used for connecting to twitter API and redirecting tweets to localhost:port

spark.py
-Creates socket text stream on localhost:port
-Takes keywords using Socket.io
-Filters the stream with respect to keywords
-Runs two neural networks both RNN and RNN-LSTM to classify sentiment in the tweet.



