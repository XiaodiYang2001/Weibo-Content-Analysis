import datetime
import json
import re
from scrapy import Spider, Request
from spiders.common import parse_tweet_info, parse_long_tweet, url_to_mid, parse_time, parse_user_info

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
        keywords = ['坠落的审判北大', '坠落的审判北大首映', '北大首映']
        start_time = datetime.datetime(year=2024, month=3, day=24, hour=21)
        end_time = datetime.datetime(year=2024, month=3, day=29, hour=21)
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
                yield Request(url, callback=self.parse_tweet, meta={'keyword': response.meta['keyword'], 'tweet_id': tweet_id}, priority=10)
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
        item['type'] = 'post'
        item['keyword'] = response.meta['keyword']
        tweet_id = response.meta['tweet_id']
        if item['isLongText']:
            url = "https://weibo.com/ajax/statuses/longtext?id=" + item['mblogid']
            yield Request(url, callback=parse_long_tweet, meta={'item': item}, priority=20)
        else:
            yield item
        
        # 爬取评论
        mid = url_to_mid(tweet_id)
        comment_url = f"https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={mid}&is_show_bulletin=2&is_mix=0&count=20"
        yield Request(comment_url, callback=self.parse_comments, meta={'source_url': comment_url, 'tweet_id': tweet_id}, priority=20)
        
        # 爬取转发内容
        repost_url = f"https://weibo.com/ajax/statuses/repostTimeline?id={mid}&page=1&moduleID=feed&count=10"
        yield Request(repost_url, callback=self.parse_reposts, meta={'page_num': 1, 'mid': mid}, priority=20)
        

    def parse_comments(self, response, **kwargs):
        """
        解析评论
        """
        data = json.loads(response.text)
        for comment_info in data['data']:
            item = self.parse_comment(comment_info)
            yield item
            # 解析二级评论
            if 'more_info' in comment_info:
                url = f"https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={comment_info['id']}" \
                      f"&is_show_bulletin=2&is_mix=1&fetch_level=1&max_id=0&count=100"
                yield Request(url, callback=self.parse_comments, priority=20)
        if data.get('max_id', 0) != 0 and 'fetch_level=1' not in response.url:
            url = response.meta['source_url'] + '&max_id=' + str(data['max_id'])
            yield Request(url, callback=self.parse_comments, meta=response.meta)

    @staticmethod
    def parse_comment(data):
        """
        解析comment
        """
        item = dict()
        item['type'] = 'comment'
        item['created_at'] = parse_time(data['created_at'])
        item['_id'] = data['id']
        item['like_counts'] = data['like_counts']
        item['ip_location'] = data.get('source', '')
        item['content'] = data['text_raw']
        item['comment_user'] = parse_user_info(data['user'])
        if 'reply_comment' in data:
            item['reply_comment'] = {
                '_id': data['reply_comment']['id'],
                'text': data['reply_comment']['text'],
                'user': parse_user_info(data['reply_comment']['user']),
            }
        return item
        
    def parse_reposts(self, response, **kwargs):
        """
        解析转发内容
        """
        data = json.loads(response.text)
        for tweet in data['data']:
            item = parse_tweet_info(tweet)
            item['type'] = 'repost'
            yield item
        if data['data']:
            mid, page_num = response.meta['mid'], response.meta['page_num']
            page_num += 1
            url = f"https://weibo.com/ajax/statuses/repostTimeline?id={mid}&page={page_num}&moduleID=feed&count=10"
            yield Request(url, callback=self.parse_reposts, meta={'page_num': page_num, 'mid': mid})
