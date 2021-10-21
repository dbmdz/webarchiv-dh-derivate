import json
import re
from datetime import datetime
from json import JSONDecodeError
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urlparse, parse_qs, unquote

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    MapType,
    IntegerType,
)


def extract_tweets(
    mime_type: str, content: str
) -> List[
    Tuple[
        str,
        str,
        Optional[str],
        str,
        List[str],
        List[str],
        List[str],
        Dict[str, Optional[int]],
    ]
]:
    """
    Extract tweets from HTML and JSON documents crawled with webrecorder.
    :param mime_type: MIME type of the document to be parsed
    :param content: document content
    :return: list of tweets with publication time, author, text, hashtags, users mentioned, link destinations
     and statistics of user reactions
    """
    if mime_type == "text/html":
        html = content
    else:
        try:
            json_content = json.loads(content)
            html = json_content["items_html"]
        except JSONDecodeError:
            return None
    soup = BeautifulSoup(html, "html.parser")
    tweet_containers = soup.find_all("div", class_="tweet")
    tweets = []
    for tweet in tweet_containers:
        author = tweet["data-screen-name"]
        if tweet.has_attr("data-retweeter"):
            retweeter = tweet["data-retweeter"]
        else:
            retweeter = None
        time = get_tweet_time(tweet)
        text = get_tweet_text(tweet)
        hashtags = get_hashtags(tweet)
        mentioned_users = get_mentioned_users(tweet)
        link_destinations = get_link_destinations(tweet)
        action_stats = get_action_stats(tweet)
        tweets.append(
            (
                time,
                author,
                retweeter,
                text,
                hashtags,
                mentioned_users,
                link_destinations,
                action_stats,
            )
        )
    return tweets


def get_hashtags(tweet: BeautifulSoup) -> Set[str]:
    hashtag_links = tweet.select('a[href^="/hashtag/"]')
    hashtags = []
    for link in hashtag_links:
        hashtag = link["href"]
        hashtag = unquote(hashtag)
        hashtag = re.sub("/hashtag/", "", hashtag)
        hashtag = re.sub("\\?[^?]*$", "", hashtag)
        if hashtag not in hashtags:
            hashtags.append(hashtag)
    return hashtags


def get_action_stats(tweet: BeautifulSoup) -> Dict[str, int]:
    """
    Get the number of replies, retweets and favorites for a tweet.
    :param tweet:
    :return: Dictionary with the keys "reply", "retweet" and "favorite", the values are the numbers of the respective actions.
    """
    actions = ["reply", "retweet", "favorite"]
    action_stats = {}
    for action in actions:
        action_class = "ProfileTweet-action--" + action
        action_obj = tweet.find("span", class_=action_class)
        if action_obj is not None:
            action_count_obj = action_obj.find(
                "span", class_="ProfileTweet-actionCount"
            )
            action_count = action_count_obj["data-tweet-stat-count"]
            action_stats[action] = int(action_count)
    return action_stats


def get_tweet_time(tweet: BeautifulSoup) -> str:
    """Get publication time of tweet in UTC."""
    time_obj = tweet.find("span", class_="_timestamp", attrs={"data-time": True})
    time = int(time_obj["data-time"])
    if time:
        utc_dt = datetime.utcfromtimestamp(time)
        time_str = utc_dt.strftime("%Y%m%d%H%M")
    return time_str


def get_tweet_text(tweet: BeautifulSoup) -> str:
    content = tweet.find("p", class_="tweet-text")
    if content:
        text = content.get_text()
    else:
        text = None
    return text


def get_mentioned_users(tweet: BeautifulSoup) -> List[str]:
    """Get twitter users mentioned in tweet."""
    user_links = tweet.find_all("a", class_="twitter-atreply", attrs={"href": True})
    mentioned_users = []
    for link in user_links:
        user = link["href"]
        user = re.sub("/", "", user)
        if user not in mentioned_users:
            mentioned_users.append(user)
    return mentioned_users


def get_link_destinations(tweet: BeautifulSoup) -> List[str]:
    """Get original, not shortened URLs which the tweet links to."""
    link_tags = tweet.find_all("a", attrs={"href": True, "data-expanded-url": True})
    link_dests = []
    for link in link_tags:
        link_dest = link["data-expanded-url"]
        if link_dest not in link_dests:
            link_dests.append(link_dest)
    return link_dests


def aggregate_action_stats(action_stats: List[Dict[str, int]]) -> Dict[str, int]:
    """Aggregate statistics by returning largest value observed for each of the tweet reactions (reply, retweet, favorite)."""
    action_stat = {}
    for key in action_stats[0].keys():
        counts = [count[key] for count in action_stats]
        action_stat[key] = max(counts)
    return action_stat


def parse_facebook_links(dest: str) -> str:
    query = urlparse(dest).query
    if query is not None:
        parsed_query = parse_qs(query)
        if "u" in parsed_query.keys():
            dest = parsed_query["u"]
    return dest


extract_tweets_udf = udf(
    extract_tweets,
    ArrayType(
        StructType(
            [
                StructField("tweet_time", StringType()),
                StructField("tweet_author", StringType()),
                StructField("retweeter", StringType()),
                StructField("tweet_text", StringType()),
                StructField("hashtags", ArrayType(StringType())),
                StructField("mentioned_users", ArrayType(StringType())),
                StructField("link_destinations", ArrayType(StringType())),
                StructField("action_stats", MapType(StringType(), IntegerType())),
            ]
        )
    ),
)

aggregate_action_stats_udf = udf(
    aggregate_action_stats, MapType(StringType(), IntegerType())
)

parse_facebook_links_udf = udf(parse_facebook_links, StringType())
