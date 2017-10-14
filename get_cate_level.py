#! /usr/bin/python
# -*- coding: utf8 -*-

import sys
import requests
from bs4 import BeautifulSoup
import re
import string
import pymongo

reload(sys)
sys.setdefaultencoding('utf8')

headers = {
    'Host': 'sou.zhaopin.com',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8',
}

client = pymongo.MongoClient('localhost', 27017)
DB_ZhaoPin = client['zhaopin']
FirstLevel_Table = DB_ZhaoPin['FirstLevel']
SecondLevel_Table = DB_ZhaoPin['SecondLevel']

url_prfex = 'http://sou.zhaopin.com'

escape_dict={'\a':r'\a',
          '\b':r'\b',
         '\c':r'\c',
         '\f':r'\f',
         '\n':r'\n',
         '\r':r'\r',
         '\t':r'\t',
         '\v':r'\v',
         '\'':r'\'',
         '\"':r'\"',
         '\0':r'\0',
         '\1':r'\1',
         '\2':r'\2',
         '\3':r'\3',
         '\4':r'\4',
         '\5':r'\5',
         '\6':r'\6',
         '\7':r'\7',
         '\8':r'\8',
         '\9':r'\9'}

def raw(text):  #将每个可能的转义字符都进行了替换
    """Returnsa raw string representation of text"""
    new_string=''
    for char in text:
       try:
           new_string += escape_dict[char]
       except KeyError:new_string+=char
    return new_string

def get_first_level_cate(url):
    counter = 0
    try:
        wb_data = requests.get(url, headers=headers)
        if wb_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(wb_data.text, 'lxml')
            all_hrefs = soup.select('#search_bottom_content_demo a')
            for href in all_hrefs:
                href_str = href.get('href')
                c = href_str.count('&', 0, len(href_str))
                if c == 1:
                    data = {
                        'name': href.get_text(),
                        'href': url_prfex + href_str
                    }
                    FirstLevel_Table.insert_one(data)
                    counter += 1
            print('共生成' + str(counter) + '条记录')
        else:
            print('获取网页数据失败===>:' + str(wb_data.status_code))
    except:
        raise
    # return first_level


def get_all_levels(url):
    first_level_counter = 0
    try:
        wb_data = requests.get(url, headers=headers)
        if wb_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(wb_data.text, 'lxml')
            all_hrefs = soup.select('#search_bottom_content_demo a')
            for href in all_hrefs:
                href_str = href.get('href')
                if href_str.count('&', 0, len(href_str)) == 1:
                    data = {
                        'name': href.get_text(),
                        'href': url_prfex + href_str
                    }
                    FirstLevel_Table.insert_one(data)
                    first_level_counter += 1
                    print('生成一级条目:' + href.get_text())
                    second_level_counter = 0
                    for item in all_hrefs:
                        item_str = item.get('href')
                        if item_str.find(href_str) >= 0 and item_str.count('&', 0, len(item_str)) == 2:
                            data = {
                                'name': item.get_text(),
                                'href': url_prfex + item_str
                            }
                            SecondLevel_Table.insert_one(data)
                            second_level_counter += 1
                            print('======>生成二级条目:' + item.get_text())
                    print('======>生成' + str(second_level_counter) + '条二级条目')
            print('共生成' + str(first_level_counter) + '条一级条目')
        else:
            print('获取网页数据失败===>:' + str(wb_data.status_code))
    except:
        raise


if __name__ =='__main__':
    main_page_url = 'http://sou.zhaopin.com/'
    get_all_levels(main_page_url)
