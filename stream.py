from config import credentials
from dateutil import parser
from twitter import OAuth, TwitterStream
import tweetstream
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
import smtplib
import traceback
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


def run_tweetstream():
    words = ['outsidelands', 'outside lands']
    try:
        print "Setting up FilterStream..."
        with tweetstream.FilterStream(credentials['username'],
                                    credentials['password'],
                                    track=words) as stream:

            print "Done."
            counter = 0
            print "Starting loop-di-loop..."
            for tweet in stream:
                print "Got tweet..."
                if tweet.get('text'):
                    print "It's an actual tweet: %s" % (tweet.get('text'))

                    counter += 1

                    user = tweet['user']
                    u = Session.query(User).filter(User.id == user['id']).first()
                    if not u:
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

                    # Commit every 20 tweets
                    if counter >= 20:
                        Session.commit()
                        print "Commiting..."
                        counter = 0
    except:
        print "Commiting due to exception..."
        Session.commit()
        send_error_email()

def send_error_email():
    fromaddr = 'hudnall.mark@gmail.com'
    toaddr = 'me@markhudnall.com'
    msg = """
    Hey! There was an error running stream.py. It's on you to check it out ands see if we need a restart.

    Traceback:
    %s
    """ % (traceback.format_exc())
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(credentials['email'], credentials['email_pass'])
    server.sendmail(fromaddr, toaddr, msg)
    server.quit()


def run_python_twitter():
    twitter_stream = TwitterStream(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET,
                                              CONSUMER_KEY, CONSUMER_SECRET))

    tweets = twitter_stream.statuses.filter(track=['twitter'])

    counter = 0
    for tweet in tweets:
        print "Got a tweet......."
        if tweet.get('text'):
            print "Got tweet..."
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

            # Commit every 20 tweets
            if counter >= 20:
                Session.commit()
                counter = 0


if __name__ == '__main__':
    run_tweetstream()
