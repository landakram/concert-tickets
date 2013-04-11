from config import credentials
from dateutil import parser
from pprint import pprint
from twitter import OAuth, TwitterStream
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
)

import json

engine = create_engine('sqlite:///test.db', echo=True)
Base = declarative_base(bind=engine)
Session = scoped_session(sessionmaker(engine))

OAUTH_TOKEN = credentials['oauth_token']
OAUTH_SECRET = credentials['oauth_secret']
CONSUMER_KEY = credentials['consumer_key']
CONSUMER_SECRET = credentials['consumer_secret']

class Tweet(Base):
    __tablename__ = 'tweet'

    id = Column(Integer, primary_key=True)
    text = Column(String)
    user_id = Column(Integer)
    favorite_count = Column(Integer)
    created_at = Column(DateTime, index=True)
    retweet_count = Column(Integer)
    truncated = Column(Boolean)

    coordinates = Column(String)
    place = Column(String)

class User(Base):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    screen_name = Column(String)
    followers_count = Column(Integer)

if __name__ == '__main__':

    twitter_stream = TwitterStream(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET,
                                              CONSUMER_KEY, CONSUMER_SECRET))

    tweets = twitter_stream.statuses.filter(track=['outsidelands',
                                                   'outside lands'])

    counter = 0
    for tweet in tweets:
        if tweet.get('text'):
            pprint(tweet.get('text'))
            counter += 1

            user = tweet['user']
            u = User(id=user['id'],
                    screen_name=user['screen_name'],
                    followers_count=user['followers_count'])
            t = Tweet(id=tweet['id'],
                    text=tweet['text'],
                    user_id=u.id,
                    favorite_count=tweet['favorite_count'],
                    created_at=parser.parse(tweet['created_at']),
                    retweet_count=tweet['retweet_count'],
                    truncated=tweet['truncated'],
                    coordinates=json.dumps(tweet['coordinates']),
                    place=json.dumps(tweet['place']))

            Session.add(u)
            Session.add(t)

            # Commit every 50 tweets
            if counter >= 50:
                Session.commit()
                counter = 0

