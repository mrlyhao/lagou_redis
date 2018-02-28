# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from fake_useragent import UserAgent

class RedistestSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class RandomUserAgentMiddlware(object):
    # 随机更换user-agent
    def __init__(self,crawler):
        super(RandomUserAgentMiddlware,self).__init__()#super的作用是获取父类的初始方法，这里是获取一个类方法
        self.ua = UserAgent()#这里是引进的一个随机UA的模块
        self.ua_type = crawler.settings.get('RANDOM_UA_TYPE','random')

    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler)#获取crawler，其中包括setting

    def process_request(self,request,spider):
        def get_ua():
            return getattr(self.ua,self.ua_type)#getattr函数可以根据传递的后边参数的不同，获取前边函数的不同值方法，类似与'.'
        random_ua = get_ua()
        request.headers.setdefault('User_Agent',random_ua)