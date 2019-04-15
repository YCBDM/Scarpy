import MySQLdb
import MySQLdb.cursors

class MysqlExportPipline(object):
    #采用同步机制将数据写入mysql；
    def __init__(self):
        self.conn = MySQLdb.connect('localhost','root','test123456','mysql',charset ='utf8',use_unicode = True)
        self.cursor = self.conn.cursor()
    def process_item(self, item, spider):
        insert_sql = """
            insert into bangkaispider(author,time,content, )
            VALUES (%s, %s, %s,)
        """
        self.cursor.execute(insert_sql,(item["author"],item["time"],item["content"],))
        self.conn.commit()
        #这里的exexute和commit是同步操作一条语句不执行完不会执行下一条语句。但是我们scrapy的解析速度很快如果入库的速度很慢的话就会造成阻塞。
    def spider_closed(self,spider):
        self.conn.close()
