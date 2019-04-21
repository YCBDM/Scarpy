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
from scrapy.pipelines.images import ImagesPipeline
from kafka import KafkaProducer
import json
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from scrapy.http import Request
from .utils.common import Kafka_producer
import logging

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
        es.index(
            index="hikbidtada",
            doc_type="Article",
            body={
                "title": item['title'],
                "author": item['author'],
                "publish_time": item['publish_time'],
                "image_urls": item['image_urls'],
                "images": item['images'],
                # "item_content":item['item_content'],
                "item_time": item['item_time'],
                "article_html": item['article_html'],
            })
        return item


class FastDFSPipleline(object):
    # 将图片存入FastDFS并将在img标签中添加属性：FastDFS服务器的地址
    # 连接FastDFS,注意：client.conf的路径非常重要，在这里踩了一天的坑；参考方案：http://www.pianshen.com/article/7745199875/

    def __init__(self):
        self.path = "D:/FastDFS_pic/"  # 图片保存路径
        self.page = 0       # 图片名我用数字代替
        if not os.path.exists(self.path):  # 检查文件是否存在
            os.mkdir(self.path)

    def reporthook(self,a, b, c):
        """
        显示下载进度
        :param a: 已经下载的数据块
        :param b: 数据块的大小
        :param c: 远程文件大小
        :return: None
        """
        print("\rdownloading: %5.1f%%" % (a * b * 100.0 / c), end="")

    def download_for_uploadtoFastDFS(self, img_urllist,dfs_client):
        """
        :param img_urllist:原来图片地址列表
        :param dfs_client:Fast_DFS的客户端
        :return: FastDFS_urllist   返回FastDFS的地址列表
        """

        for url in img_urllist:
            work_path = self.path
            FastDFS_urllist = []  # 用来装FastDFS地址
            try:
                # 下载图片
                urllib.request.urlretrieve(
                    url, work_path,
                         self.page, self.reporthook)
                # 将本地图片上传到FastDFS
                result = dfs_client.upload_by_filename(
                    "D:/FastDFS_pic/%s" % self.page)
                # 获取FastDFS路径
                Fastdfs_url = result.get(
                    'Storage IP') + '\\' + result.get('Remote file_id')
                # 将每一次取到的FastDFS地址存入FastDFS_urllist
                FastDFS_urllist.append(Fastdfs_url)
                self.page += 1
            except Exception as e:
                print(e)
        logging.info(FastDFS_urllist)
        return FastDFS_urllist

    def replace_url(self,item,FastDFS_urllist):
        """
        将原来网页中的地址替换成FastDFS返回的图片地址
        :param FastDFS_urllist：要替换的地址列表，用来替换原来html文档中img的src值
        :return: 返回替换后的html文档：FastDFS_article(response.text格式的)
        """
        soup = BeautifulSoup(
            item['article_html'],
            'html.parser',
            from_encoding='utf-8')
        imgurlset = soup.find(id="js_content").find_all(name='img')
        for eachimgurl, eachfasturl in zip(imgurlset, FastDFS_urllist):
            eachimgurl['src'] = eachfasturl
        FastDFS_article = soup.prettify()  # str格式的html,整个替换后的文档
        logging.info(FastDFS_article)
        return FastDFS_article



    def process_item(self, item, spider):
        # 启动Fastdfs客户端
        dfs_client = Fdfs_client('C:/Program Files/Python37/client.conf')
        # 下载并上传图片到FastDFS
        for src in item['image_urls']:

            FastDFS_urllist = []  # 用来装FastDFS地址
            try:
                # 下载图片
                urllib.request.urlretrieve(
                    src, "D:/FastDFS_pic/%s" %
                    self.page, self.reporthook)
                # 将本地图片上传到FastDFS
                result = dfs_client.upload_by_filename(
                    "D:/FastDFS_pic/%s" % self.page)
                # 获取FastDFS路径
                Fastdfs_url = result.get(
                    'Storage IP') + '\\' + result.get('Remote file_id')
                # 将每一次取到的FastDFS地址存入FastDFS_urllist
                FastDFS_urllist.append(Fastdfs_url)
                self.page += 1
            except Exception as e:
                print(e)
        logging.info("?????????????????????????????????????????????????????")
        logging.info(FastDFS_urllist)

        soup = BeautifulSoup(
            item['article_html'],
            'html.parser',
            from_encoding='utf-8')
        imgurlset = soup.find(id="js_content").find_all(name='img')
        for eachimgurl, eachfasturl in zip(imgurlset, FastDFS_urllist):
            eachimgurl['src'] = eachfasturl
        FastDFS_article = soup.prettify()  # str格式的html,整个替换后的文档
        logging.info(FastDFS_article)
        # 将item里面的文章替换掉
        FastDFS_item = item
        FastDFS_item['article_html'] = FastDFS_article

        # 记得删除下载的图片
        # os.remove("D:/FastDFS_pic/")
        # 最后将FastDFS_item传给kafka
        producer = Kafka_producer("10.197.236.154,10.197.236.169,10.197.236.184", 9092, "HikBigdata")
        params = json.dumps(dict(FastDFS_item))
        paramsitem = json.dumps(dict(item))
        print(params)
        producer.sendjsondata(params)
        producer.sendjsondata(paramsitem)
        producer.producer.close(timeout=10)
        return item


class DownloadPipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None):
        """
        重写ImagesPipeline类的file_path方法
        实现：下载下来的图片命名是以校验码来命名的，该方法实现保持原有图片命名
        :return: 图片路径
        """
        image_guid = request.url.split('/')[-1]  # 取原url的图片命名
        return 'full/%s' % (image_guid)

    def get_media_requests(self, item, info):
        """
        遍历image_urls里的每一个url，调用调度器和下载器，下载图片
        :return: Request对象
        图片下载完毕后，处理结果会以二元组的方式返回给item_completed()函数
        """
        for image_url in item['image_urls']:
            print(image_url)
            yield Request(image_url)

    def item_completed(self, results, item, info):
        """
        将图片的本地路径赋值给item['image_paths']
        :param results:下载结果，二元组定义如下：(success, image_info_or_failure)。
        第一个元素表示图片是否下载成功；第二个元素是一个字典。
        如果success=true，image_info_or_error词典包含以下键值对。失败则包含一些出错信息。
        字典内包含* url：原始URL * path：本地存储路径 * checksum：校验码
        :param item:
        :param info:
        :return:
        """
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")  # 如果没有路径则抛出异常
        item['image_paths'] = image_paths
        return item
