#! /usr/bin/python
# -*- coding: utf8 -*-

import sys
import requests
from bs4 import BeautifulSoup
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
Detail_Table = DB_ZhaoPin['DetailUrls']
ItemInfo_Table = DB_ZhaoPin['ItemInfos']
SecondLevel_Table_tmp = DB_ZhaoPin['SecondLevel_tmp']

url_prfex = 'http://sou.zhaopin.com'

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
            print('共抓取' + str(counter) + '条一级条目')
        else:
            print('获取网页数据失败===>:' + str(wb_data.status_code))
    except:
        raise
    # return first_level

#抓取所有一级和二级目录，存入数据库中
def get_all_levels(url):
    first_level_counter = 0
    second_level_total_count = 0
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
                                'firstLevelName': href.get_text(),
                                'secondLevelName': item.get_text(),
                                'href': url_prfex + item_str
                            }
                            SecondLevel_Table.insert_one(data)
                            second_level_counter += 1
                            second_level_total_count += 1
                            print('======>生成二级条目:' + item.get_text())
                    print('======>生成' + str(second_level_counter) + '条二级条目')
            print('共生成' + str(first_level_counter) + '条一级条目.' + '生成' + str(second_level_total_count) + '条二级条目.')
        else:
            print('获取网页数据失败===>:' + str(wb_data.status_code))
    except:
        raise


#某一页是否有下一页
def has_next_page(url, headers):
    try:
        s = requests.session()
        s.keep_alive = False
        wb_data = s.get(url, headers=headers)
        if wb_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(wb_data.text, 'lxml')
            next_button = soup.select('li.pagesDown-pos > a[href]')
            if len(next_button) > 0:
                return next_button[0].get('href')
            else:
                return None
        else:
            print('获取网页数据失败,返回码：' + str(wb_data.status_code))
            return None
    except:
        raise
        return None


#通过一个合法url，抓取条目列表url，存入数据库
def get_detail_url_list_from(url, firstLevelName, secondLevelName, headers):
    try:
        s = requests.session()
        s.keep_alive = False
        wb_data = s.get(url, headers=headers)
        if wb_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(wb_data.text, 'lxml')
            hrefs = soup.select('table.newlist td.zwmc > div > a:nth-of-type(1)')
            for href in hrefs:
                data = {
                    'firstLevelName': firstLevelName,
                    'secondLevelName': secondLevelName,
                    'name': href.get_text(),
                    'href': href.get('href')
                }
                Detail_Table.insert_one(data)
            return len(hrefs)
        else:
            print('获取网页数据失败,返回码：' + str(wb_data.status_code))
            return None
    except:
        raise



#抓取某二级Url下的所有条目连接url
def get_all_detail_url_from(secondLevelUrl,firstLevelName, secondLevelName, headers):
    pageNum = 1
    totalRecs = 0
    print('获取第' + str(pageNum) + '页数据...')
    totalRecs += get_detail_url_list_from(secondLevelUrl,firstLevelName, secondLevelName, headers)
    print('完成!')
    next_page_url = has_next_page(secondLevelUrl,headers)


    while next_page_url:
        pageNum += 1
        print('获取第' + str(pageNum) + '页数据...')
        totalRecs += get_detail_url_list_from(next_page_url,firstLevelName, secondLevelName, headers)
        print('完成!')
        next_page_url = has_next_page(next_page_url, headers)

    print('共抓取' + str(totalRecs) + '条数据！')

#通过遍历二级条目表，获取所有明细url，存入数据库DetailUrls中
def get_all_item_url(headers):
    loaded_url = []
    for i in list(SecondLevel_Table_tmp.find({}, {'_id': 0})):
        loaded_url.append(i['href'])
    for item in SecondLevel_Table.find():
        firstLevelName = item['firstLevelName']
        secondLevelName = item['secondLevelName']
        href = item['href']
        if href in loaded_url > 0:
            continue
        else:
            #为防止由于上次抓取异常存在的冗余数据，先做一次删除
            Detail_Table.remove({'firstLevelName': firstLevelName, 'secondLevelName': secondLevelName})
            try:
                print('正在抓取:==>' + firstLevelName + '<==  ==>' + secondLevelName + '<==的数据...')
                get_all_detail_url_from(href, firstLevelName=firstLevelName, secondLevelName=secondLevelName, headers=headers)
                data = {
                    'href': href
                }
                SecondLevel_Table_tmp.insert_one(data)
                loaded_url.append(href)
                print('完成抓取:==>' + firstLevelName + '<==  ==>' + secondLevelName + '<==的数据...')
            except:
                raise




#通过DetailUrls数据表中的url抓取条目明细,存入数据库中
def get_all_item_info_from(headers):
    for item in Detail_Table.find().limit(100):
        name = item['name']
        firstLevelName = item['firstLevelName']
        secondLevelName = item['secondLevelName']
        href = item['href']
        # print(item['name'], item['href'], item['firstLevelName'], item['secondLevelName'])
        try:
            s = requests.session()
            s.keep_alive = False
            wb_data = s.get(href, headers=headers)
            if wb_data.status_code == requests.codes.ok:
                pass
            else:
                print('获取网络数据失败, 返回码:' + str(wb_data.status_code))
        except:
            raise



'''计算量统计部分'''
#获得某Url下的所有条目数
def get_count_of(url, headers):
    try:
        wb_data = requests.get(url, headers=headers)
        if wb_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(wb_data.text, 'lxml')
            hrefs = soup.select('table.newlist td.zwmc > div > a:nth-of-type(1)')
            return len(hrefs)
        else:
            print('获取网页数据失败,返回码：' + str(wb_data.status_code))
            return None
    except:
        raise

# 抓取某二级Url下的所有条目数
def get_all_count_from(secondLevelUrl, headers):
    pageNum = 1
    totalRecs = 0
    # print('获取第' + str(pageNum) + '页数据...')
    totalRecs += get_count_of(secondLevelUrl,  headers)
    # print('完成!')
    next_page_url = has_next_page(secondLevelUrl, headers)

    while next_page_url:
        pageNum += 1
        # print('获取第' + str(pageNum) + '页数据...')
        totalRecs += get_count_of(next_page_url, headers)
        # print('完成!')
        next_page_url = has_next_page(next_page_url, headers)

    return totalRecs


#计算某一级条目数据量
def cal_total_count_from(firstLevelName, headers):
    if FirstLevel_Table.find({'name': firstLevelName}).count() > 0:
        total_count = 0
        print('正在获取一级条目：' + firstLevelName + '的数据量...')
        for item in SecondLevel_Table.find({"firstLevelName": firstLevelName}):
            secondLevelName = item['secondLevelName']
            secondUrl = item['href']
            print('=====> 获取二级条目：<<<' + secondLevelName + '>>>条目数...')
            current_count = get_all_count_from(secondUrl, headers)
            total_count = current_count + total_count
            print('=====> 获取二级条目成功：' + str(current_count) + '条.' )
        print('获取一级条目成功：' + str(total_count) + '条.' )
        return total_count
    else:
        print('没有此一级条目：' + firstLevelName)

#计算总数据量
def cal_total_count(headers):
    total_count = 0
    for item in SecondLevel_Table.find():
        firstLevelName = item['firstLevelName']
        secondLevelName = item['secondLevelName']
        secondUrl = item['href']
        print('一级条目：' + firstLevelName + '==> 二级条目：' + secondLevelName + '条目数...')
        current_count = get_all_count_from(secondUrl, headers)
        total_count = current_count + total_count
        print('当前条目数：' + str(current_count) + '当前所有条目数:' + str(total_count))


    return total_count

if __name__ == '__main__':
    # main_page_url = 'http://sou.zhaopin.com/'
    # get_all_levels(main_page_url)

    # pass

    # url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=530&bj=7002000&sj=464'
    # print(has_next_page(url, headers))
    #get_detail_url_list_from(url,'销售行政/商务','其他',headers)
    # get_all_detail_url_from(url, '销售行政/商务','其他',headers)



    #计算所有条目数
    #print('所有计算量：' + str(cal_total_count(headers)))
    #计算一级条目数
    # cal_total_count_from('销售业务', heade

    # get_all_item_url(headers)

    print Detail_Table.count()

