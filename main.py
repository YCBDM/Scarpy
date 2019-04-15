# _*_ coding: utf-8 _*_
# 此脚本主要做调试用
__author__ = 'yincaibing'
__date__ = '2019/4/2 15:02'

from scrapy.cmdline import execute

import sys
import os

# 将系统当前目录设置为项目根目录
# os.path.abspath(__file__)为当前文件所在绝对路径
# os.path.dirname为文件所在目录
# D:\yincaibing\BangkaiSpider\main.py

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# 执行命令，相当于在控制台cmd输入改名了
execute(["scrapy", "crawl" , "csdn"])
