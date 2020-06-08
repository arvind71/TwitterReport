import tweepy
import csv
import pandas as pd
import re
from textblob import TextBlob
from wordcloud import WordCloud,STOPWORDS
import matplotlib.pyplot as plt
import datetime
import time 
import flask
import pickle
import os

import seaborn as sns
    
from sklearn.linear_model import SGDClassifier
#from sklearn.externals import joblib 
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfTransformer
import joblib
sgd1 = joblib.load('LRintents1.pkl') 
sgd2 = joblib.load('Sentiment1.pkl') 

consumer_key = 'iRR48Zo57CPp046T2eylDxS09'
consumer_secret = 'iMk1yKClvqGTLkjUrRQUF5UB6tfmfxBdq5UKgh1E33u9fR7oDQ'
access_token = '1644277225-bmRJBS3VyJTgvyMCAoteYHk7hCedggUK1iebgHP'
access_token_secret = 't6xsXYQEVXMO6HfOvB81sOWIWBPPEaXDvHT76tsnnO74q'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
#MYDIR = os.path.dirname(__file__)
#PEOPLE_FOLDER = os.path.join('static', 'report')
app = flask.Flask(__name__)
#app.config['UPLOAD_FOLDER'] = PEOPLE_FOLDER
#img_path =os.path.join(MYDIR + "/" + app.config['UPLOAD_FOLDER'])
@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    
    return r
@app.route("/")
def mypage():
    return flask.render_template('index.html')
@app.route("/get_report",methods = ['POST'])
def report():
    #PEOPLE_FOLDER = os.path.join('static', 'report')
    #app.config['UPLOAD_FOLDER'] = PEOPLE_FOLDER
    # username = "narendramodi"
    # startDate = datetime.datetime(2020, 2, 25, 0, 0, 0)
    # endDate =   datetime.datetime(2020, 5, 6, 0, 0, 0)
    #input_name = flask.request.form['u']
    username = flask.request.form['ScreenName']
    print(username)
    startDate = flask.request.form['StartDate']
    startDate = datetime.datetime.strptime(startDate,'%Y-%m-%d')
    print(startDate)

    endDate = flask.request.form['EndDate']
    endDate = datetime.datetime.strptime(endDate,'%Y-%m-%d')

    print(endDate)

    #startDate = datetime.datetime(startDate)
    #endDate =   datetime.datetime(endDate)

    tweets = []
    created_at = []
    likes = []
    retweets = []
    retweeted =[]
    tweet_id =[]
    # verified_retweeters = []
    tmpTweets = api.user_timeline(username,tweet_mode="extended",exclude_replies=True,trim_user = True)
    # print(tmpTweets[7])
    for tweet in tmpTweets:
        if tweet.created_at < endDate and tweet.created_at > startDate:
            if (tweet.full_text[:2] == 'RT'):
                tweets.append(tweet.full_text)
                created_at.append(tweet.created_at)
                likes.append(tweet.favorite_count)
                retweets.append(0)
                tweet_id.append(tweet.id)
                retweeted.append("YES")
                
                
                
                
            else:
                tweets.append(tweet.full_text)
                created_at.append(tweet.created_at)
                likes.append(tweet.favorite_count)
                retweets.append(tweet.retweet_count)
                tweet_id.append(tweet.id)
                retweeted.append("NO")

    #         print("=====================================")

    while (tmpTweets[-1].created_at > startDate):
        tmpTweets = api.user_timeline(username, max_id = tmpTweets[-1].id,tweet_mode="extended",exclude_replies=True,trim_user = True)
        for tweet in tmpTweets:
            if tweet.created_at < endDate and tweet.created_at > startDate:
                if (tweet.full_text[:2] == 'RT'):
                    tweets.append(tweet.full_text)
                    created_at.append(tweet.created_at)
                    likes.append(tweet.favorite_count)
                    retweets.append(0)
                    tweet_id.append(tweet.id)
                    retweeted.append("YES")

                else:
                    tweets.append(tweet.full_text)
                    created_at.append(tweet.created_at)
                    likes.append(tweet.favorite_count)
                    retweets.append(tweet.retweet_count)
                    tweet_id.append(tweet.id)
                    retweeted.append("NO")
    #             print("++++++++++++++++++++++++++++++++++")
                
    print(len(tweets))
    print(len(created_at))
    df = pd.DataFrame(list(zip(tweets, created_at,likes,retweets,retweeted,tweet_id)), 
                columns =['Tweets', 'created_at','likes','retweets','retweeted','tweet_id']) 
    
    def cleanTxt(text):
        text = re.sub(r'@[A-Za-z0-9_]+',' ',text) #remove @ mentioned
        text = re.sub(r'#',' ',text) #removing the # symbol
        text = re.sub(r'RT[\s]+',' ',text) #removing RT
        text = re.sub(r'https?:\/\/.*[\r\n]*', ' ', text, flags=re.MULTILINE)
        text = re.sub(r':',' ',text) #removing the : symbol
        text = re.sub(r'\n',' ',text) #removing the # symbol
        text = re.sub(r'&amp',' ',text)
    
        return text
    df['Tweets'] = df['Tweets'].apply(cleanTxt)
    def get_intent(text):
        
        intent= sgd1.predict([text])


        return intent[0]
    def get_sentiment(text):
        
        sentiment= sgd2.predict([text])


        return sentiment[0]
    df['Intent'] = df['Tweets'].apply(get_intent)
    df['Sentiment'] = df['Tweets'].apply(get_sentiment)
    STOP_LIST = ['न', 'तरी', 'तो', 'हें', 'तें', 'कां', 'आणि', 'जें', 'जे', 'मग', 'ते', 'मी', 
             'जो', 'परी', 'गा', 'हे', 'ऐसें', 'आतां', 'तैसें', 'परि', 'नाहीं', 'तेथ', 'हा', 
             'तया', 'असे', 'म्हणे', 'काय', 'म्हणौनि', 'कीं', 'जैसें', 'तंव', 'तूं', 'होय', 
             'जैसा', 'आहे', 'पैं', 'तैसा,जरी', 'म्हणोनि,एक', 'ऐसा', 'जी', 'ना', 'मज', 
             'एथ', 'या', 'जेथ', 'जया', 'तुज', 'तेणें', 'तैं', 'पां', 'असो', 'करी', 'ऐसी', 
             'येणें', 'जाहला,तेंचि', 'आघवें', 'होती', 'जैंकांहीं', 'होऊनि', 'एकें', 'मातें', 'ठायीं',
             'ये', 'अर्जुना', 'सकळ', 'केलें', 'जेणें', 'जाण', 'जैसी', 'होये', 'जेवीं', 'एऱ्हवीं', 
             'मीचि', 'किरीटी', 'दिसे', 'देवा', 'हो', 'तरि', 'कीजे', 'तैसे', 'आपण', 'तिये', 
             'कर्म', 'नोहे', 'इये', 'पडे', 'पार्था', 'माझें', 'तैसी', 'लागे', 'नाना', 'जंव', 'कीर',
             'आह','आज', 'असं', 'बरं', 'बर', 'कर', 'आपलं', 'आपल','तर','आण','viewpoint',
             'delhi','watch','today','will','now','फक','वर','coronaviru' ]
    path = './lohit-devanagari/Lohit-Devanagari.ttf'
    stopwords = set(STOPWORDS)
    stopwords=list(stopwords)
    stopwords=set(stopwords+STOP_LIST)
    #stopwords.add('CoronaVirusUpdate')
    #stopwords.add('Covid_19')
    #print(stopwords)
    allWords = ' '.join([twts for twts in df['Tweets']])

    wordCloud = WordCloud(background_color="white",font_path = path,stopwords = stopwords).generate(allWords)
    plt.figure(figsize=(10,6))

    plt.imshow(wordCloud,interpolation = "bilinear")
    plt.axis("off")
    plt.savefig("./static/report/Wordcloud.png")
    #full_filename1 = os.path.join(app.config['UPLOAD_FOLDER'], 'Wordcloud.png')
    
    plt.figure(figsize=(12,5))
    sns.set(rc={"axes.facecolor":"#283747", "axes.grid":False,'xtick.labelsize':14,'ytick.labelsize':14,},font='Liberation Serif')
    #sns.set(style="darkgrid",palette='deep',font='Liberation Serif')
    ax = sns.countplot(y="Intent", data=df,palette="Set1")

    plt.savefig("./static/report/Bargraph.png",bbox_inches = 'tight')
    #plt.savefig("output.png")
    #full_filename2 = os.path.join(app.config['UPLOAD_FOLDER'], 'Bargraph.png')
    
    
    # sns.set(rc={"axes.facecolor":"#283747"},font='Liberation Serif')

    #fig1 = plt.gcf()
    #fig = plt.figure(figsize=(8,6))
    df.groupby('Sentiment').Tweets.count().plot.pie(figsize=(8,8),autopct="%1.1f%%",
                                                title = 'Sentiments PieChart',
                                                explode=[0, 0, 0.05],colors = ['maroon','orange','green'],
                                                shadow=True,
                                               )
                                                
    plt.legend()
    #plt.show()
    plt.draw()
    plt.savefig("./static/report/Piechart.png",
                bbox_inches = 'tight')
    #full_filename3 = os.path.join(app.config['UPLOAD_FOLDER'], 'Piechart.png')
    
    df1 = df[df['retweeted']=='NO']
    df1['date'] = df1['created_at'].dt.date

    # d = df1.groupby(df1['created_at'],as_index=False)['likes','retweets'].sum()
    df1 = df1.groupby('date', as_index=False)['likes','retweets'].sum()
#fig = plt.figure(figsize=(18,5))
    fig = plt.figure()
    sns.set(rc={"axes.facecolor":"#283747", "axes.grid":True,'xtick.labelsize':15,'ytick.labelsize':15})
    plt.title("Likes and Retweet count according to dates",fontsize = 14)
    plt.yticks(rotation=50)
    #sns.lineplot(x = df1.index.values, y = canada['India'] , color = '#ff9900' , label= 'Ind
    df1.set_index('date')['likes'].plot(figsize=(12, 5),linewidth=2, color='orange',marker = 'o')
    df1.set_index('date')['retweets'].plot(figsize=(20, 10),linewidth=2, color='blue',marker = 'o')
    #fig2 = plt.gcf()
    plt.legend(facecolor= 'grey' , fontsize='large' , edgecolor = 'black' ,shadow=True)
    #plt.legend()
    # plt.show()
    plt.draw()
    fig.tight_layout()
    plt.savefig("./static/report/Linechart.png")
    #full_filename4 = os.path.join(app.config['UPLOAD_FOLDER'], 'Linechart.png')
    url1 = "/static/report/Wordcloud.png"
    url2 = "/static/report/Bargraph.png"
    url3 = "/static/report/Piechart.png"
    url4 = "/static/report/Linechart.png"
    return flask.render_template('report.html',user_image1 = url1,user_image2 = url2,
                                  user_image3 = url3, user_image4 = url4)
if __name__ == "__main__":
   
    app.run(debug = True)
