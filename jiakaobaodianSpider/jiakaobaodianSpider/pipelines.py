# -*- coding: utf-8 -*-
from twisted.enterprise import adbapi
import json
import MySQLdb
import MySQLdb.cursors
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class JiakaobaodianspiderPipeline(object):
    def __init__(self):
        # 打开文件
        self.filename = open("jiakaobaodian.json", "w")

    def process_item(self, item, spider):
        # 将获取到的每条item转换为json格式
        text = json.dumps(dict(item), ensure_ascii=False) + ",\n"
        self.filename.write(text)
        return item

    def close_spider(self, spider):
        # 关闭文件
        self.filename.close()


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
                    insert into jiakaobaodianSpider(item_question,item_answer,item_answeranalyse,item_pic,item_chooseslist) VALUES (%s,%s,%s,%s,%s)
                """
        cursor.execute(
            insert_sql,
            (''.join(item["item_question"]),
             item["item_answer"],
            ''.join(item["item_answeranalyse"]),
            item['item_pic'],
            ''.join(item['item_chooseslist']),
             )
        )



