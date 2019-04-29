# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JiakaobaodianspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    item_question = scrapy.Field()
    item_answer = scrapy.Field()
    item_answeranalyse = scrapy.Field()
    item_pic = scrapy.Field()
    item_content = scrapy.Field()
    item_chooseslist = scrapy.Field()
    item_time = scrapy.Field()