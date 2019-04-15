# -*- coding: utf-8 -*-

# Define here the models for your scraped items
# 定义爬取数据的容器
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import scrapy

class BangkaispiderItem(scrapy.Item):
    # define the fields for your item here like:
    # content = scrapy.Field()
    # img = scrapy.Field()
    # url = scrapy.Field()
    # tags = scrapy.Field()
    # url_object_id = scrapy.Field()
    # front_image_path = scrapy.Field()
     title = scrapy.Field()
     author = scrapy.Field()
     publish_time = scrapy.Field()
     img_url = scrapy.Field()
     img_title = scrapy.Field()
     img_tag = scrapy.Field()
     img_name = scrapy.Field()





