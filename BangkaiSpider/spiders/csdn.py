# -*- coding: utf-8 -*-
import scrapy
from ..items import BangkaispiderItem
from urllib import parse
class CsdnSpider(scrapy.Spider):
    # 爬CSDN
    name = 'csdn'
    allowed_domains = ['blog.csdn.net']
    start_urls = ['https://blog.csdn.net/csdnsevenn/article/details/89166748']


    def parse(self, response):
        # 实例化自己爬取数据的容器
        item = BangkaispiderItem()
        selector = scrapy.Selector(response)
        """
        url = 获取所有文章的链接
        author = 作者
        publish_time = 发布时间
        content = 原文
        keyword= 原文关键字
        img_url = 图片链接
        img_title = 图片标题
        img_tag = 图片标签
        alt =  图片的名字
        """
        item['author'] = response.xpath('//a[@class="follow-nickName"]/text()').extract()[0]
        item['publish_time'] = response.xpath('//span[@class="time"]/text()').extract()[0]
        item['title'] = response.xpath('//h1[@class="title-article"]/text()').extract()[0]
        item['img_url'] = selector.xpath('//*[@id="js_content"]/ p[2] / img/@src').extract()
        item['img_name'] = selector.xpath('//*[@id="js_content"]/ p[2] / img/@alt').extract()
        # js_content > p:nth-child(3) > img
        #// *[ @ id = "js_content"] / p[2] / img
        #image_src = selector.xpath('//*[@id="js_content"]/ p[2] / img/@src').extract()

        yield item


