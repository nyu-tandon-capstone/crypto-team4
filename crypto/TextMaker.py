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
    MAX_SLEEP = 20  # 6
    LIMIT_TYPE = "backoff"  # "average"
    JITTER = "full"
    BASE_BACKOFF = 0.2
    LIMIT = 50000
    DOWN_BEHAVIOR = None  # 'warn', 'stop'
    DAY_S = 86400
    STEP = 86400 * 1
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
            user_agent=f'python: PMAW request enrichment (by u/{auth_config.get("data").get("username")})',
            # ratelimit_seconds=60
        )
        self.api = PushshiftAPI(num_workers=self.NUM_WORKERS, praw=self.reddit, shards_down_behavior=self.DOWN_BEHAVIOR,
                                limit_type=self.LIMIT_TYPE, jitter=self.JITTER, base_backoff=self.BASE_BACKOFF,
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
        texts['date'] = pd.to_datetime(texts.epoch, unit='s').dt.date

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
                file_scheme='hive',
                write_index=False,
                partition_on=["date", "subreddit"],
                append=True
            )
        else:
            fastparquet.write(
                os.path.join(text_path, filename), texts,
                compression="snappy",
                file_scheme='hive',
                write_index=False,
                partition_on=["date", "subreddit"]
            )

    def fetch_text_union(self, channel):
        s = self.start
        e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)

        bar = tqdm(total=int((self.end + self.DAY_S - self.start)/self.STEP))
        while s < self.end + self.DAY_S - 1:
            bar.set_description(str(pd.to_datetime(s, unit='s')) + ": posts")
            post = self.search_post(channel, s, e)
            post = post.sort_values("created_utc")
            if len(post) >= self.LIMIT:
                bar.write(f"{str(pd.to_datetime(s, unit='s'))}: post count {len(post)} > {self.LIMIT}")
            self.save_text(post, 'p')

            bar.set_description(str(pd.to_datetime(s, unit='s')) + ": comments")
            comment = self.search_comment_by_post(post.id.to_list())
            comment = comment.sort_values("created_utc")
            if len(comment) >= self.LIMIT:
                bar.write(f"{str(pd.to_datetime(s, unit='s'))}: comment count {len(comment)} > {self.LIMIT}")
            self.save_text(comment, 'c')

            bar.update(1)
            s = e + 1
            e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)

        bar.close()

    def fetch_text(self, channel, text_type=None):
        if text_type == 'c':
            search_func = self.search_comment
        elif text_type == 'p':
            search_func = self.search_post
        else:
            raise Exception(f"text_type {text_type} not supported")

        s = self.start
        e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)
        bar = tqdm(total=int((self.end + self.DAY_S - self.start)/self.STEP))
        while s < self.end + self.DAY_S - 1:
            bar.set_description(str(pd.to_datetime(s, unit='s')))
            df = search_func(channel, s, e)
            df = df.sort_values("created_utc")
            if len(df) >= self.LIMIT:
                bar.write(f"{str(pd.to_datetime(s, unit='s'))}: {len(df)} > {self.LIMIT}")
            self.save_text(df, text_type)
            bar.update(1)

            s = e + 1
            e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)

        bar.close()

    def fetch_comment_by_post(self, channel):
        s = self.start
        e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)
        bar = tqdm(total=int((self.end + self.DAY_S - self.start) / self.STEP))
        posts = fastparquet.ParquetFile(os.path.join(text_path, "reddit/posts.parquet"))

        while s < self.end + self.DAY_S - 1:
            bar.set_description(str(pd.to_datetime(s, unit='s').date()))

            post_df = posts.to_pandas(
                columns=['id', 'num_comments', 'subreddit'],
                filters=[('date', '==', str(pd.to_datetime(s, unit='s').date())),
                         ('subreddit', 'in', channel.split(','))]
            )
            ids = []
            for sub in channel.split(','):
                sub_df = post_df[(post_df.subreddit == sub) & (post_df.num_comments != 0)]
                cutoff = sub_df.num_comments.quantile(0.9)
                ids.extend(sub_df[sub_df.num_comments > cutoff].id.to_list())

            num_posts = len(ids)
            num_comments = post_df[post_df.id.isin(ids)].num_comments.sum()
            bar.set_description(f"{str(pd.to_datetime(s, unit='s').date())}: {num_posts}-{num_comments}")
            comment = self.search_comment_by_post_r(post_ids=ids)
            comment = comment.sort_values("created_utc")
            bar.write(f"{str(pd.to_datetime(s, unit='s').date())}: {num_posts}-{num_comments}-{len(comment)}")
            self.save_text(comment, 'c')
            del post_df

            bar.update(1)
            s = e + 1
            e = min(s + self.STEP - 1, self.end + self.DAY_S - 1)

        bar.close()

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

    def search_comment_by_post_r(self, post_ids: list):
        post_ids = ["t3_"+i for i in post_ids]

        comments = []
        post_iter = self.reddit.info(fullnames=post_ids)
        for post in post_iter:
            comment_obj = post.comments
            comment_obj.replace_more(limit=48)
            comments.extend(comment_obj.list())

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

    def search_comment_by_post(self, post_ids: list):
        comments = self.api.search_submission_comment_ids(
            ids=post_ids,
            limit=self.LIMIT,
        )

        f_items = itemgetter(*self.COMMENT_COLUMNS)
        comment_list = [f_items(comment) for comment in comments if self.__comment_filter(comment)]

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


# if __name__ == '__main__':
#     c = RedditMaker([], "2019-03-18", "2019-03-19")
#     c.fetch_text("CryptoCurrency,CryptoMarkets,Bitcoin,BNBTrader", 'p')  # cryptocurrency,Bitcoin,BNBTrader
    # c.save_text(df, 'c')
    # df = c.search_post("cryptocurrency", 1631232000, 1631318400)
    # print('x')
    # print(a.head())
    # print(b.head())
    #
    # print(a.shape)
    # print(b.shape)
