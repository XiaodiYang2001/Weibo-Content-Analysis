# -*- coding: utf-8 -*-
"""Untitled0.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1T36XaVy34zMroNTrYxuBjPOe3o6jdLbW
"""

import datetime
import json
import re
from scrapy import Spider, Request
from spiders.common import parse_tweet_info, parse_long_tweet, parse_user_info, parse_time, url_to_mid

class TweetSpiderByKeyword(Spider):
    """
    关键词搜索采集
    """
    name = "tweet_spider_by_keyword"
    base_url = "https://s.weibo.com/"

    def start_requests(self):
        """
        爬虫入口
        """
        # 这里keywords可替换成实际待采集的数据
        keywords = ['北大首映', '坠落的审判北大', '坠落的审判北大首映']
        # 这里的时间可替换成实际需要的时间段
        start_time = datetime.datetime(year=2024, month=3, day=24, hour=21)
        end_time = datetime.datetime(year=2024, month=3, day=29, hour=21)
        # 是否按照小时进行切分，数据量更大; 对于非热门关键词**不需要**按照小时切分
        is_split_by_hour = True
        for keyword in keywords:
            if not is_split_by_hour:
                _start_time = start_time.strftime("%Y-%m-%d-%H")
                _end_time = end_time.strftime("%Y-%m-%d-%H")
                url = f"https://s.weibo.com/weibo?q={keyword}&timescope=custom%3A{_start_time}%3A{_end_time}&page=1"
                yield Request(url, callback=self.parse, meta={'keyword': keyword})
            else:
                time_cur = start_time
                while time_cur < end_time:
                    _start_time = time_cur.strftime("%Y-%m-%d-%H")
                    _end_time = (time_cur + datetime.timedelta(hours=1)).strftime("%Y-%m-%d-%H")
                    url = f"https://s.weibo.com/weibo?q={keyword}&timescope=custom%3A{_start_time}%3A{_end_time}&page=1"
                    yield Request(url, callback=self.parse, meta={'keyword': keyword})
                    time_cur = time_cur + datetime.timedelta(hours=1)

    def parse(self, response, **kwargs):
        """
        网页解析
        """
        html = response.text
        if '<p>抱歉，未找到相关结果。</p>' in html:
            self.logger.info(f'no search result. url: {response.url}')
            return
        tweets_infos = re.findall('<div class="from"\s+>(.*?)</div>', html, re.DOTALL)
        for tweets_info in tweets_infos:
            tweet_ids = re.findall(r'weibo\.com/\d+/(.+?)\?refer_flag=1001030103_" ', tweets_info)
            for tweet_id in tweet_ids:
                url = f"https://weibo.com/ajax/statuses/show?id={tweet_id}"
                yield Request(url, callback=self.parse_tweet, meta=response.meta, priority=10)
        next_page = re.search('<a href="(.*?)" class="next">下一页</a>', html)
        if next_page:
            url = "https://s.weibo.com" + next_page.group(1)
            yield Request(url, callback=self.parse, meta=response.meta)

    def parse_tweet(self, response):
        """
        解析推文
        """
        data = json.loads(response.text)
        item = parse_tweet_info(data)
        item['keyword'] = response.meta['keyword']
        if item['isLongText']:
            url = "https://weibo.com/ajax/statuses/longtext?id=" + item['mblogid']
            yield Request(url, callback=parse_long_tweet, meta={'item': item}, priority=20)
        else:
            yield item

        # 获取微博的唯一标识符 mid
        mid = data.get('mid')  # 假设微博数据中的唯一标识符为 'mid'
        if mid:
            # 构造评论信息的请求
            comments_url = f"https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={mid}&is_show_bulletin=2&is_mix=0&count=20"
            yield Request(comments_url, callback=self.parse_comments, meta={'item': item, 'source_url': comments_url}, priority=30)
            # 抓取转发信息
            reposts_url = f"https://weibo.com/ajax/statuses/repostTimeline?id={mid}&page=1&moduleID=feed&count=10"
            yield Request(reposts_url, callback=self.parse_reposts, meta={'item': item, 'page_num': 1, 'mid': mid}, priority=30)
            # 构造点赞信息的请求
            attitudes_url = f"https://weibo.com/ajax/statuses/attitudes?id={mid}&page=1&count=20"
            yield Request(attitudes_url, callback=self.parse_attitudes, meta={'item': item, 'page_num': 1, 'mid': mid}, priority=30)
        else:
            self.logger.warning(f"No 'mid' found in tweet data. URL: {response.url}")

        yield item

    def parse_comments(self, response):
        """
        解析评论内容
        """
        item = response.meta['item']
        data = json.loads(response.text)
        item['comments'] = [{'user_id': comment['user']['id'], 'nick_name': comment['user']['screen_name'], 'comment': comment['text_raw']} for comment in data['data']]
        yield item

        # 解析二级评论
        for comment_info in data['data']:
            if 'more_info' in comment_info:
                url = f"https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={comment_info['id']}" \
                      f"&is_show_bulletin=2&is_mix=1&fetch_level=1&max_id=0&count=100"
                yield Request(url, callback=self.parse_comments, priority=20)

        if data.get('max_id', 0) != 0 and 'fetch_level=1' not in response.url:
            url = response.meta['source_url'] + '&max_id=' + str(data['max_id'])
            yield Request(url, callback=self.parse_comments, meta=response.meta)

    def parse_reposts(self, response):
        """
        解析转发内容
        """
        item = response.meta['item']
        data = json.loads(response.text)
        if 'reposts' not in item:
            item['reposts'] = []
        item['reposts'].extend([{'user_id': repost['user']['id'], 'nick_name': repost['user']['screen_name'], 'repost': repost['text_raw']} for repost in data['data']])
        yield item

    def parse_attitudes(self, response):
        """
        解析点赞用户
        """
        item = response.meta['item']
        data = json.loads(response.text)
        if 'attitudes' not in item:
            item['attitudes'] = []
        item['attitudes'].extend([{'user_id': attitude['user']['id'], 'nick_name': attitude['user']['screen_name']} for attitude in data['data']])
        yield item
