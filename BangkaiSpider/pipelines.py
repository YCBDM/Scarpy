# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import MySQLdb.cursors
import codecs
import json
from twisted.enterprise import adbapi
from scrapy.exporters import JsonItemExporter
from scrapy.exporters import JsonLinesItemExporter
from w3lib.html import remove_tags
from elasticsearch import Elasticsearch
from fdfs_client.client import Fdfs_client
import requests

# scrapy自带的写入本地json文件的类


class JsonExportPipline(object):
    def __init__(self):
        self.file = open('articleexport.json', 'wb')
        self.exporter = JsonItemExporter(
            self.file, encoding="utf-8", ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        print(dict(item))
        return item


# 采用Twisted框架提供的api接口进行mysql数据入库的异步操作！
class MysqlTwistedPipline(object):
    # 异步机制将数据写入mysql；
    def __init__(self, dbpool):
        self.dbpool = dbpool
    # 通过@classmethod这个方法定义一个from_settings函数（自定义主键和扩展的时候很有用！）
    # 被scrapy调用然后取再settings.py中设置的MYSQL的值
    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(host=settings["MYSQL_HOST"],
                       db=settings["MYSQL_DBNAME"],
                       user=settings["MYSQL_USER"],
                       passwd=settings["MYSQL_PASSWORD"],
                       charset="utf8",
                       cursorclass=MySQLdb.cursors.DictCursor,
                       use_unicode=True,
                       )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)
        return cls(dbpool)
# 上面两个函数主要用来传递dbpool

    def process_item(self, item, spider):
        # 使用twisted将mysql插入变成一部执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error)  # 处理异常

    def handle_error(self, failure):
        # 处理一部插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        # 执行具体的插入
        insert_sql = """
                    insert into bangkaispider(author,time)
                    VALUES (%s, %s)
                """
        cursor.execute(insert_sql, (
            item["author"], item["publish_time"]),item["title"])


class ElasticsearchPipeline(object):
    # 将数据写入到ES中
    def process_item(self, item, spider):
        # 将item转换为ES的数据
        es = Elasticsearch('10.197.236.171')
        print(item)
        es.index(index="hikbidtada", doc_type="Article", body={"title": item['title'],
                                                               "author": item['author'],
                                                                "publish_time": item['publish_time'],
                                                               "img_url":item['img_url'],
                                                               })

        return item


class FastDFSPipleline(object):
    # 将图片存入FastDFS并将源地址替换为FastDFS服务器的地址
    def process_item(self, item, spider):
        # 连接FastDFS,注意：client.conf的路径非常重要，在这里踩了一天的坑；参考方案：http://www.pianshen.com/article/7745199875/
        dfs_client = Fdfs_client('C:/Program Files/Python37/client.conf')
        print(dfs_client)
        # ret = dfs_client.upload_by_filename('timg.jpg')

        # 通过图片名称上传
        # dfs_client.upload_by_filename('name')
        # 通过二进制的方式上传
        # response =  dfs_client.upload_by_buffer('‘文件bytes数据')
        # print("this is FastDFS response "+ ret)
        return item



