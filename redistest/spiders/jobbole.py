import scrapy
from scrapy_redis.spiders import RedisSpider
import re
from scrapy.http import Request
from urllib import parse
from scrapy.loader import ItemLoader
import datetime
from selenium import webdriver
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import time


class MySpider(RedisSpider):
    name = 'jobbole'
    redis_key = 'jobbole:start_urls'
    allowed_domains = ['jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']
    # def __init__(self):
    #     self.browser = webdriver.Chrome(executable_path='C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')
    #     super(CommentSpider,self).__init__()
    #     dispatcher.connect(self.spider_closed,signals.spider_closed)
    #
    # def spider_closed(self,spider):9
    #     # 当爬虫退出的时候关闭chrome
    #     print('Spider closed')
    #     self.browser.quit()
    # 收集伯乐在线所有404的url及404页面数
    handle_httpstatus_list = [404]
    def parse(self, response):
        if response.status == 404:
            self.fail_url.append(response.url)
            self.crawler.stats.inc_value("failed_url")
        post_urls = response.css('.post.floated-thumb .post-thumb a:nth-child(1)')
        for post_url in post_urls:
            url = post_url.css('::attr(href)').extract_first('')
            image_url = post_url.css('img::attr(src)').extract()
            yield Request(url = parse.urljoin(response.url,url),meta={'front_image_url':image_url},callback=self.parse_detail)
        next_url = response.css('.next.page-numbers::attr(href)').extract_first()
        if next_url:
            yield Request(url = next_url,callback=self.parse)
    def parse_detail(self,response):
        pass