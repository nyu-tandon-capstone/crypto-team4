import json
import os
from operator import itemgetter

import pandas as pd
import praw
from praw.models import MoreComments
from bs4 import BeautifulSoup
from pmaw import PushshiftAPI
from tqdm import tqdm
import fastparquet

from crypto.utils import reddit_auth_path
from crypto.utils import text_path


class RedditMaker:
    NUM_WORKERS = 22
    RATE_LIMIT = 85
    MAX_SLEEP = 6
    LIMIT = 20000
    DOWN_BEHAVIOR = None  # 'warn', 'stop'
    DAY_S = 86400
    STEP = 86400 * 1
    CHUNK_SIZE = 1 * LIMIT
    POST_COLUMNS = [
        "subreddit", "subreddit_subscribers",
        "id", "permalink", "url", "created_utc", "title", "selftext", "selftext_html",
        "num_comments", "score", "downs", "ups", "upvote_ratio"
    ]
    COMMENT_COLUMNS = [
        "subreddit", "link_id", "parent_id",
        "id", "permalink", "created_utc", "body", "body_html",
        "score", "downs", "ups"
    ]

    def __init__(self, channels, start, end):
        with open(reddit_auth_path, "r") as file:
            auth_config = json.load(file)

        self.reddit = praw.Reddit(
            client_id=auth_config.get("CLIENT_ID"),
            client_secret=auth_config.get("SECRET_TOKEN"),
            user_agent=f'python: PMAW request enrichment (by u/CRH2002A)'
        )
        self.api = PushshiftAPI(num_workers=self.NUM_WORKERS, praw=self.reddit, shards_down_behavior=self.DOWN_BEHAVIOR,
                                rate_limit=self.RATE_LIMIT, max_sleep=self.MAX_SLEEP)

        self.channels = channels
        self.start = pd.to_datetime(start).timestamp()
        self.end = pd.to_datetime(end).timestamp()

    @staticmethod
    def __post_filter(item):
        return item["selftext"] not in ["[deleted]", "[removed]", '']

    @staticmethod
    def __comment_filter(item):
        return item["body"] not in ["[deleted]", "[removed]", '']

    @staticmethod
    def __comment_filter_praw(item):
        return item.body not in ["[deleted]", "[removed]", '']

    @staticmethod
    def __clean_href(text):
        soup = BeautifulSoup(text, 'html.parser')
        for a in soup.findAll('a', href=True):
            a.extract()

        try:
            soup = soup.find('div')
        except:
            pass

        ps = [p.get_text(strip=True, separator=' ') for p in soup.find_all()]
        return ' '.join(ps)

    @staticmethod
    def save_text(texts, text_type=None):
        texts = texts.rename(columns={"created_utc": "epoch"})

        if text_type == 'c':
            texts = texts.astype({
                "epoch": int,
                "score": int,
                "downs": int,
                "ups": int,
            })
            filename = "reddit/comments.parquet"
        elif text_type == 'p':
            texts = texts.astype({
                "epoch": int,
                "subreddit_subscribers": int,
                "num_comments": int,
                "score": int,
                "downs": int,
                "ups": int,
                "upvote_ratio": float
            })
            filename = "reddit/posts.parquet"
        else:
            raise Exception(f"text_type {text_type} not supported")

        if os.path.exists(os.path.join(text_path, filename)):
            fastparquet.write(
                os.path.join(text_path, filename), texts,
                compression="snappy",
                write_index=False,
                partition_on=["subreddit"],
                append=True
            )
        else:
            fastparquet.write(
                os.path.join(text_path, filename), texts,
                compression="snappy",
                write_index=False,
                partition_on=["subreddit"]
            )

    def fetch_text_union(self, channel):
        s = self.start
        e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)
        # count_post = 0
        # count_comment = 0

        # posts = pd.DataFrame(columns=self.POST_COLUMNS)
        comments = pd.DataFrame(columns=self.COMMENT_COLUMNS)

        bar = tqdm(total=int((self.end + self.DAY_S - self.start)/self.STEP))
        while s < self.end + self.DAY_S - 1:
            bar.set_description(str(pd.to_datetime(s, unit='s')) + ": posts")
            post = self.search_post(channel, s, e)
            post = post.sort_values("created_utc")
            if len(post) >= self.LIMIT:
                bar.write(f"{str(pd.to_datetime(s, unit='s'))}: post count {len(post)} > {self.LIMIT}")
            # posts = pd.concat([posts, post], axis=0, ignore_index=True)
            # count_post += len(post)

            bar.set_description(str(pd.to_datetime(s, unit='s')) + ": comments")
            for post_id in post.id.to_list():
                comment = self.search_comment_by_post(post_id)
                comment = comment.sort_values("created_utc")
                comments = pd.concat([comments, comment], axis=0, ignore_index=True)
                # count_comment += len(comment)

            self.save_text(post, 'p')
            self.save_text(comments, 'c')
            comments = pd.DataFrame(columns=self.COMMENT_COLUMNS)

            # if count_post > self.CHUNK_SIZE:
            #     self.save_text(posts, 'p')
            #     posts = pd.DataFrame(columns=self.POST_COLUMNS)
            #     count_post = 0
            # if count_comment > self.CHUNK_SIZE:
            #     self.save_text(comments, 'c')
            #     comments = pd.DataFrame(columns=self.COMMENT_COLUMNS)
            #     count_comment = 0

            bar.update(1)
            s = e + 1
            e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)

        bar.close()
        # if count_post > 0:
        #     self.save_text(posts, 'p')
        # if count_comment > 0:
        #     self.save_text(comments, 'c')
        # return posts, comments

    def fetch_text(self, channel, text_type=None):
        if text_type == 'c':
            search_func = self.search_comment
            columns = self.COMMENT_COLUMNS
        elif text_type == 'p':
            search_func = self.search_post
            columns = self.POST_COLUMNS
        else:
            raise Exception(f"text_type {text_type} not supported")

        s = self.start
        e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)
        count = 0
        bar = tqdm(total=int((self.end + self.DAY_S - self.start)/self.STEP))
        texts = pd.DataFrame(columns=columns)
        while s < self.end + self.DAY_S - 1:
            bar.set_description(str(pd.to_datetime(s, unit='s')))
            df = search_func(channel, s, e)
            df = df.sort_values("created_utc")
            if len(df) >= self.LIMIT:
                bar.write(f"{str(pd.to_datetime(s, unit='s'))}: {len(df)} > {self.LIMIT}")
            texts = pd.concat([texts, df], axis=0, ignore_index=True)
            count += len(df)
            bar.update(1)

            if count > self.CHUNK_SIZE:
                self.save_text(texts, text_type)
                texts = pd.DataFrame(columns=columns)
                count = 0

            s = e + 1
            e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)

        bar.close()
        if count > 0:
            self.save_text(texts, text_type)
        # return texts

    def search_post(self, channel, start, end):
        posts = self.api.search_submissions(
            subreddit=channel,
            limit=self.LIMIT,
            filter_fn=self.__post_filter,
            after=int(start),
            before=int(end)
        )

        f_items = itemgetter(*self.POST_COLUMNS)
        post_list = [f_items(post) for post in posts]

        post_df = pd.DataFrame(post_list, columns=self.POST_COLUMNS)
        post_df["selftext_clean"] = post_df["selftext_html"].apply(self.__clean_href)
        post_df["subreddit"] = post_df["subreddit"].apply(lambda x: x.display_name)

        return post_df

    def search_comment_by_post(self, post_id):
        post = self.reddit.submission(post_id)
        comments = post.comments
        comments.replace_more()
        comments = comments.list()

        comment_list = []
        for comment in comments:
            if isinstance(comment, MoreComments):
                # print('more')
                continue
            if not self.__comment_filter_praw(comment):
                # print('filtered')
                continue
            comment_list.append(
                [comment.subreddit, comment.link_id, comment.parent_id,
                 comment.id, comment.permalink, comment.created_utc, comment.body, comment.body_html,
                 comment.score, comment.downs, comment.ups]
            )

        comment_df = pd.DataFrame(comment_list, columns=self.COMMENT_COLUMNS)
        comment_df["body_clean"] = comment_df["body_html"].apply(self.__clean_href)
        comment_df["subreddit"] = comment_df["subreddit"].apply(lambda x: x.display_name)

        return comment_df

    def search_comment(self, channel, start, end):
        comments = self.api.search_comments(
            subreddit=channel,
            limit=self.LIMIT,
            filter_fn=self.__comment_filter,
            after=int(start),
            before=int(end)
        )

        f_items = itemgetter(*self.COMMENT_COLUMNS)
        comment_list = [f_items(comment) for comment in comments]

        comment_df = pd.DataFrame(comment_list, columns=self.COMMENT_COLUMNS)
        comment_df["body_clean"] = comment_df["body_html"].apply(self.__clean_href)
        comment_df["subreddit"] = comment_df["subreddit"].apply(lambda x: x.display_name)

        return comment_df


if __name__ == '__main__':
    c = RedditMaker([], "2018-01-01", "2018-01-02")
    c.fetch_text_union("CryptoMarkets")  #cryptocurrency,Bitcoin,
    # c.save_text(df, 'c')
    # df = c.search_post("cryptocurrency", 1631232000, 1631318400)
    print('x')
    # print(a.head())
    # print(b.head())
    #
    # print(a.shape)
    # print(b.shape)
