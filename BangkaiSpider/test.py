from fdfs_client.client import Fdfs_client
import sys,os





if __name__ == '__main__':
    dfs_client = Fdfs_client('C:/Program Files/Python37/client.conf')
    ret = dfs_client.upload_by_filename('timg.jpg')
    pic_url = ret.get('Storage IP') + '\\' + ret.get('Remote file_id')
    print(pic_url)
    print("这是个图片地址：" + str(pic_url))


