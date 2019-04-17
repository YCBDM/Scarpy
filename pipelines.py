# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import MySQLdb.cursors
from urllib.request import urlretrieve
import os
import urllib.request
from bs4 import BeautifulSoup
import re
from twisted.enterprise import adbapi
from scrapy.exporters import JsonItemExporter
from scrapy.exporters import JsonLinesItemExporter
from w3lib.html import remove_tags
from elasticsearch import Elasticsearch
from fdfs_client.client import Fdfs_client
from kafka import KafkaProducer
import json


global page     # 全局定义page

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
        cursor.execute(
            insert_sql,
            (item["author"],
             item["publish_time"]),
            item["title"],
            item['img_url'],
            item['img_name'])


class ElasticsearchPipeline(object):

    # 将数据写入到ES中
    def process_item(self, item, spider):
        # 将item转换为ES的数据
        es = Elasticsearch('10.197.236.171')
        print(es)
        es.index(
            index="hikbidtada",
            doc_type="Article",
            body={
                "title": item['title'],
                "author": item['author'],
                "publish_time": item['publish_time'],
                "img_url": item['img_url'],
                "img_name": item['img_name'],
                "item_content":item['item_content'],
                "item_time":item['item_time'],
            })
        print("this is item type which in ES!!!!!!"+ type(item['item_time']))
        return item


class FastDFSPipleline(object):
    # 将图片存入FastDFS并将源地址替换为FastDFS服务器的地址
    # 连接FastDFS,注意：client.conf的路径非常重要，在这里踩了一天的坑；参考方案：http://www.pianshen.com/article/7745199875/

    def __init__(self):
        self.path = "D:/FastDFS_pic/"  # 图片保存路径
        self.page = 0       # 图片名我用数字代替
        if not os.path.exists(self.path):  # 检查文件是否存在
            os.mkdir(self.path)

    def process_item(self, item, spider):
        # 启动Fastdfs客户端
        dfs_client = Fdfs_client('C:/Program Files/Python37/client.conf')

        # 连接kafka
        def reporthook(a, b, c):
            """
            显示下载进度
            :param a: 已经下载的数据块
            :param b: 数据块的大小
            :param c: 远程文件大小
            :return: None
            """
            print("\rdownloading: %5.1f%%" % (a * b * 100.0 / c), end="")

        # 将FastDFS的图片地址替换成原来网页中的地址
        def replace_url():
            pass

        for src in item['img_url']:
            work_path = self.path
            try:
                # 下载图片
                urllib.request.urlretrieve(
                    src, "D:/FastDFS_pic/%s" %
                    self.page, reporthook)

                # 将本地图片上传到FastDFS
                result = dfs_client.upload_by_filename(
                    "D:/FastDFS_pic/%s" % self.page)

                # 获取FastDFS路径
                Fastdfs_url = result.get(
                    'Storage IP') + '\\' + result.get('Remote file_id')

                # 用FastDFS替换掉item中原来的img_url
                print('this is  原来的img_url:'+item['img_url'][self.page] )
                item_Fastdfs = item
                (item_Fastdfs['img_url'][self.page])=Fastdfs_url
                print('this is  替换后的Fastdfs_url:' + item_Fastdfs['img_url'][self.page])
                self.page += 1
            except Exception as e:
                print(e)
        # 记得删除下载的图片
        # pass
        # 最后将item传给kafka
        producer = KafkaProducer(
            bootstrap_servers="10.197.236.154:9092,10.197.236.169:9092,10.197.236.184:9092")
        msg = json.dumps(dict(item_Fastdfs))
        print("this is item_Fastdfs!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + str(item_Fastdfs))
        producer.send('ScarpyHikcreateBigdata', msg, partition=0)
        print("this is item_Fastdfs!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + str(item_Fastdfs))
        producer.close()
        return item
