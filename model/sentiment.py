import pandas as pd 
import numpy as np
import re
import matplotlib.pyplot as plt
import nltk
# nltk.download('stopwords')
# nltk.download('wordnet')

from nltk.tokenize import TweetTokenizer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords

from textblob import TextBlob

class tweetSentimentAnalysis:
    def __init__(self, data, stopwords, cryptowords):
        self.data = data
        self.stopwords = stopwords
        self.cryptowords = cryptowords
        self.data_preprocessing()
    
    def data_preprocessing(self):
        data = self.data.dropna(subset=['text','hashtags']).reset_index(drop=True)
        text_data = data[['text']].copy()
        text_data['text'] = text_data['text'].apply(self.text_clean)
        text_data['subjectivity'] = text_data['text'].apply(self.getSubjectivity)
        text_data['polarity'] = text_data['text'].apply(self.getPolarity)
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
        text = [t for t in text if t not in (self.stopwords + self.cryptowords)]
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

class redditSentimentAnalysis:
    def __init__(self, data, stopwords, cryptowords):
        self.data = data
        self.stopwords = stopwords
        self.cryptowords = cryptowords
        self.data_preprocessing()
    
    def data_preprocessing(self):
        data = self.data.dropna(subset=['title','selftext']).reset_index(drop=True)
        data['epoch'] = pd.to_datetime(data['epoch'], unit='s')
        text_data = data[['selftext']].copy()
        text_data['selftext'] = text_data['selftext'].apply(self.text_clean)
        text_data['subjectivity'] = text_data['selftext'].apply(self.getSubjectivity)
        text_data['polarity'] = text_data['selftext'].apply(self.getPolarity)
        text_data = pd.concat([data[['epoch','num_comments','score','upvote_ratio']], text_data],axis=1)
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
        text = [t for t in text if t not in (self.stopwords + self.cryptowords)]
        #4. lemmatize
        text = [lemmatizer.lemmatize(t) for t in text]
        #5. Joining

        return " ".join(text)

    def getSubjectivity(self, reddit):
        """
        Create a function to get subjectivity
        Subjectivity is in range [0,1]
        """
        return TextBlob(reddit).sentiment.subjectivity

    def getPolarity(self, reddit):
        """
        create a function to get the polarity
        polarity is in range [-1,1]
        """
        return TextBlob(reddit).sentiment.polarity

if __name__ == "__main__":
   print("Insider user")

