# -*- coding: utf-8 -*-
import scrapy
from ..items import JiakaobaodianspiderItem
import json
import scrapy.selector
from scrapy.http import Request
import time

class JiakaobaodianSpider(scrapy.Spider):
    # 爬驾考宝典
    name = 'jiakaobaodian'
    allowed_domains = ['http://km1.jsyst.cn/fx/q']
    url = 'http://km1.jsyst.cn/fx/q'

    def start_requests(self):
        for i in range(1, 1355):
            url = self.url + str(i)
            yield Request(url, self.parse)

    def parse(self, response):
        item = JiakaobaodianspiderItem()
        selector = scrapy.Selector(response)
        item['item_question'] = response.xpath('//h1/text()').extract()[0]
        item['item_answer'] = response.xpath('//font/b/text()').extract()
        item['item_content'] = response.xpath(
            '//div[@class="vehiclesIn3"]/p/text()').extract()
        item['item_answeranalyse'] = [item['item_content'][-1]]
        item['item_pic'] = response.xpath('//img/@src').extract()
        # item['item_time'] = time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time()))

        if item['item_pic'] is None:
            # 答案列表等于内容列表去掉尾部
            item['item_chooseslist'] = item['item_content'][:-1]
        else:
            # 答案列表等于内容列表去掉头部和尾部
            item['item_chooseslist'] = item['item_content'][0:1]
            item['item_chooseslist'] = item['item_content'][0:-1]
        yield item

