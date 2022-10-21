import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import pickle
import nltk
# nltk.download('stopwords')
# nltk.download('wordnet')

from nltk.tokenize import TweetTokenizer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from textblob import TextBlob

data_ori = pd.read_pickle('./data/Bitcoin_tweets_sample.pkl')

class SentimentAnalysis:
    def __init__(self, data, vectorizer, classifier, stopwords, crypto):
        self.data = data
        self.vectorizer = vectorizer
        self.classifier = classifier
        self.stopwords = stopwords
        self.crypto = crypto
        self.data_preprocessing()
    
    def data_preprocessing(self):
        data = self.data.dropna(subset=['text','hashtags']).reset_index(drop=True)
        text_data = data[['text']].copy()
        text_data['text'] = text_data['text'].apply(self.text_clean)
        print(text_data)
        text_data['subjectivity'] = text_data['text'].apply(self.getSubjectivity)
        text_data['polarity'] = text_data['text'].apply(self.getPolarity)
        text_data['sentiment'] = text_data['polarity'].apply(self.getSentiment)
        text_data = pd.concat([data[['date','user_followers','user_friends']], text_data],axis=1)
        self.text_data = text_data
        return self.text_data

    def text_clean(self, text):
        #I.  
        #1. Remove urls/hyperlinks
        text = re.sub(r'((www\.[^\s]+)|(http\S+))',' ', text)
        #2. Remove hashtags
        text = re.sub(r'#\w+', ' ', text)
        #3. Remove mentions 
        text = re.sub(r'@\w+',' ', text)
        #4. Remove characters that not in the English alphabets
        text = re.sub('[^A-Za-z]+', ' ', text)
        #5. Remove additional white spaces
        text = re.sub('[\s]+', ' ', text)
        #II. 
        #1. Tokenize
        text = TweetTokenizer().tokenize(text)
        #2. Lower?
        text = [l.lower() for l in text]
        #2. Remove Puncs
        text = [w for w in text if w.isalpha()]
        #3. Removing Stopwords
        lemmatizer = WordNetLemmatizer()
        # Import nltk stopwords and customize it to add common crypto words that don't add too much information 
        stop_words = self.stopwords
        crypto_words = self.crypto
        stop_words = stop_words + crypto_words
        text = [t for t in text if t not in stop_words]
        #4. lemmatize
        text = [lemmatizer.lemmatize(t) for t in text]
        #5. Joining

        return " ".join(text)

    def getSubjectivity(self, tweet):
        """
        Create a function to get subjectivity
        Subjectivity is in range [0,1]
        """
        return TextBlob(tweet).sentiment.subjectivity

    def getPolarity(self, tweet):
        """
        create a function to get the polarity
        polarity is in range [-1,1]
        """
        return TextBlob(tweet).sentiment.polarity

    def getSentiment(self, score):
        """
        A function to get sentiment text based on polarity
        """
        if score < 0:
            return 'negative'
        elif score == 0:
            return 'neutral'
        else:
            return 'positive'


new_obj = SentimentAnalysis(data=data_ori, vectorizer=TfidfVectorizer(), classifier="NaN", stopwords=stopwords.words(['english']),crypto = ['btc','bitcoin','eth','etherum','crypto'])

print(new_obj.text_data)