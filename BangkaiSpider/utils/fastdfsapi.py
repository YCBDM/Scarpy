# -*- coding:utf-8 -*-
'''
create by yincaibing
'''
from fdfs_client.client import *
import time
import logging

logger = logging.getLogger(__name__)
client_file = 'C:/Program Files/Python37/client.conf'


class Fdfs():

    def __init__(self):
        self.client = Fdfs_client(client_file)

    def upload(self, upload_file):
        try:
            ret_upload = self.client.upload_by_filename(upload_file)
            time.sleep(5)
            file_id = ret_upload['Remote file_id'].replace('\\', '/')
            return file_id

        except Exception as e:
            logger.warning(u'图片上传失败，错误信息：%s' % e.message)
            return None

    def download(self, download_file, file_id):
        try:
            ret_download = self.client.download_to_file(download_file, file_id)
            return ret_download

        except Exception as e:
            logger.warning(u'图片下载失败，错误信息：%s' % e.message)
            return None

    def delete(self, file_id):
        try:
            ret_delete = self.client.delete_file(file_id)
            return ret_delete

        except Exception as e:
            logger.warning(u'图片删除失败，错误信息：%s' % e.message)
            return None


if __name__ == '__main__':
    Fdfs.upload('timg.jpg')