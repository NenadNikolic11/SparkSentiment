from flask import Flask,jsonify,request
from flask_cors import CORS
from flask_socketio import SocketIO,emit, join_room,send
import ast

from keras.models import load_model
import time
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from gensim.models import Word2Vec
import pickle







import time
app = Flask(__name__)
CORS(app)
labels = []
values = []
user_inputs={}
user_data={}
socketio = SocketIO(app,cors_allowed_origins="*",async_mode='threading')



@socketio.on('connect')
def test_connect():
    print('connect')


@socketio.on('disconnect')
def test_disconnect():
    print('Client disconnected')
@socketio.on('json')
def handle_json(json):
    print('received json: ' + str(json))
@socketio.on('id')
def handle_my_custom_event(json):
    print('received json: ' + str(json))
    user_inputs[json['sockid']] = (" " + json['value'] + " ").lower()
    user_data[json['sockid']]=[]
    print(user_inputs)
    #emit('toclient', {'data': json['sockid']}, room=json['sockid'])
@app.route("/")
def get_chart_page():
	return jsonify(sLabel=labels, sData=values)
@app.route('/refreshData')
def refresh_graph_data():
	global labels, values
	print("labels now: " + str(labels))
	print("data now: " + str(values))
	return jsonify(sLabel=labels, sData=values)
@app.route('/updateData', methods=['POST'])
def update_data():
  global labels, values
  if not request.form or 'data' not in request.form:
    return "error",400
  labels = ast.literal_eval(request.form['label'])
  values = ast.literal_eval(request.form['data'])
  print("labels received: " + str(labels))
  print("data received: " + str(values))
  return "success",201


from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import sys
import time
import threading
from textblob import TextBlob





tracked_keywords = ["Donald Trump","#HillaryClinton", "IT", "MachineLearning"]
def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


spark = SparkSession.builder.master("local[3]").appName("Word Count").getOrCreate()

def process_line(line):
    print('runnnn')
    for key in user_inputs:
        if user_inputs[key] in line:
            with app.test_request_context():

                socketio.emit('toclient', {'data': line, 'score': 0.0}, room=key)
                print('emited')


# Spark < 2.0
# sqlContext.createDataFrame([], schema)

def process_rdd(time, rdd):
    print(time)
    #try:
    sql_context = get_sql_context_instance(rdd.context)
    collected = rdd.collect()
    print(collected)
    for line in collected:
        for key in user_inputs:
            if user_inputs[key] in line.lower():
                user_data[key].append(line)

        #row_rdd = rdd.map(lambda w: Row(text=w))
        # create a DF from the Row RDD
        #hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        #hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        #hashtag_counts_df = sql_context.sql(
            #"select text from hashtags where text like '%#DonaldTrump%'")
        #collected = hashtag_counts_df.collect()


    #except:
        #e = sys.exc_info()[0]
        #print("Error: %s" % e)
list_of_words =[]
def filter_tweets(line_text):
    for key in user_inputs:
        if user_inputs[key] in line_text.lower():
            return True
    return False


def send_data_to_users():
    w2v_model = Word2Vec.load('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\model.w2v')
    model = load_model('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\model.h5')
    with open('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\tokenizer.pkl', 'rb') as handle:
        tokenizer = pickle.load(handle)
    with open('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\encoder.pkl', 'rb') as handle:
        encoder = pickle.load(handle)
    w2v_model2 = Word2Vec.load('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\model3.w2v')
    model2 = load_model('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\model3.h5')
    with open('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\tokenizer2.pkl', 'rb') as handle:
        tokenizer2 = pickle.load(handle)
    with open('C:\\Users\\Nenad\\Desktop\\Diplomski slike\\encoder2.pkl', 'rb') as handle:
        encoder2 = pickle.load(handle)
    first_sentence = "It's the same to me whether I'm up or down"
    second_sentence = "I don't know anything"
    third_sentence = "It's ok if you like it"
    fourth_sentence = "I am really not doing well"
    fifth_sentence = "I'm neutral on the subject"
    sixth_sentence = "We are looking for a car"
    x_test1 = pad_sequences(tokenizer.texts_to_sequences([first_sentence]), maxlen=300)
    x_test2 = pad_sequences(tokenizer.texts_to_sequences([second_sentence]), maxlen=300)
    x_test3 = pad_sequences(tokenizer.texts_to_sequences([third_sentence]), maxlen=300)
    x_test4 = pad_sequences(tokenizer.texts_to_sequences([fourth_sentence]), maxlen=300)
    x_test5 = pad_sequences(tokenizer.texts_to_sequences([fifth_sentence]), maxlen=300)
    x_test6 = pad_sequences(tokenizer.texts_to_sequences([sixth_sentence]), maxlen=300)

    score = model.predict(x_test1)
    print(score)
    score = model.predict(x_test2)
    print(score)
    score = model.predict(x_test3)
    print(score)
    score = model.predict(x_test4)
    print(score)
    score = model.predict(x_test5)
    print(score)
    score = model.predict(x_test6)
    print(score)
    print("RNN")
    score2 = model2.predict(x_test1)
    print(score2)
    score2 = model2.predict(x_test2)
    print(score2)
    score2 = model2.predict(x_test3)
    print(score2)
    score2 = model2.predict(x_test4)
    print(score2)
    score2 = model2.predict(x_test5)
    print(score2)
    score2 = model2.predict(x_test6)
    print(score2)


    while True:
        for key in user_data:
            for line in user_data[key]:

                x_test = pad_sequences(tokenizer.texts_to_sequences([line]), maxlen=300)
                x_test2 = pad_sequences(tokenizer2.texts_to_sequences([line]), maxlen=300)# Predict

                score = model.predict(x_test)
                score2 = model2.predict(x_test2)
                score = float(score[0][0])
                score2=float(score2[0][0])

                socketio.emit('toclient', {'data': line, 'scoreRNN': score2, 'scoreLSTM':score}, room=key)
                user_data[key]= user_data[key][1:]
        time.sleep(2)

#thread1 = threading.Thread(target = fun1, args = (30))
#thread1.start()


conf=SparkConf()
conf.setAppName('TwitterAppSA')
conf.setMaster('local[6]')

sc = spark.sparkContext
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 10)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)



dataStream=dataStream.flatMap(lambda line: line.split("\n"))
dataStream = dataStream.filter(lambda w: filter_tweets(w))
dataStream.foreachRDD(process_rdd)


ssc.start()
thread1 = threading.Thread(target = send_data_to_users)
thread1.start()

if __name__ == "__main__":
    socketio.run(app,port=5001,host='localhost')

while True:
    time.sleep(100000)

ssc.awaitTermination()





