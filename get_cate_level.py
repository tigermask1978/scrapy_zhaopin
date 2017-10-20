#! /usr/bin/python
# -*- coding: utf8 -*-

import sys
import requests
from bs4 import BeautifulSoup
import pymongo
import re
import string
import time

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

jobs_headers = {
        'Host': 'jobs.zhaopin.com',
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
Detail_Table_tmp = DB_ZhaoPin['DetailUrls_tmp']

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


#通过url抓取一条明细
def get_one_item_info_from(url, firstLevelName, secondLevelName, headers):
    try:
        s = requests.session()
        s.keep_alive = False
        wb_data = s.get(url, headers=headers)
        if wb_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(wb_data.text, 'lxml')
            position_name = soup.select('div.fl > h1')[0].get_text()
            month_salarys = soup.select('div.terminalpage-left > ul.terminal-ul.clearfix > li:nth-of-type(1) > strong')
            if month_salarys:
                month_salary = month_salarys[0].get_text()
                pattern = r'([0-9]+)-([0-9]+)'
                # g = re.findall(pattern, month_salary)
                g = re.match(pattern, month_salary)
                if g:
                    month_salary = [g.group(1), g.group(2)]
                else:
                    month_salary = '未知薪水'
            else:
                month_salary = '未发现薪水'
            pub_dates = soup.select('#span4freshdate')
            pub_date = pub_dates[0].get_text() if pub_dates else '未知日期'
            work_experiences = soup.select('div.terminalpage-left > ul.terminal-ul.clearfix > li:nth-of-type(5) > strong')
            if work_experiences:
                work_experience = work_experiences[0].get_text()
                pattern = r'([0-9]+)-([0-9]+)'
                # g = re.findall(pattern, month_salary)
                g = re.match(pattern, work_experience)
                if g:
                    work_experience = [g.group(1), g.group(2)]
                else:
                    work_experience = '未知工作经验'
            else:
                work_experience = '未发现工作经验'
            recruit_nums = soup.select('div.terminalpage-left > ul.terminal-ul.clearfix > li:nth-of-type(7) > strong')
            if recruit_nums:
                recruit_num = recruit_nums[0].get_text()
                pattern = r'([0-9]+)'
                # g = re.findall(pattern, month_salary)招聘人数
                g = re.match(pattern, recruit_num)
                if g:
                    recruit_num = g.group(1)
                else:
                    recruit_num = '未知招聘人数'
            else:
                recruit_num = '未发现招聘人数'

            work_places = soup.select('div.terminalpage-left > ul.terminal-ul.clearfix > li:nth-of-type(2) > strong')
            work_place = work_places[0].get_text() if work_places else '未知工作地点'

            full_part_times = soup.select('div.terminalpage-left > ul.terminal-ul.clearfix > li:nth-of-type(4) > strong')
            full_part_time = full_part_times[0].get_text() if full_part_times else '未知工作性质'

            minimum_educations = soup.select('body > div.terminalpage.clearfix > div.terminalpage-left > ul > li:nth-of-type(6) > strong')
            minimum_education = minimum_educations[0].get_text() if minimum_educations else '未知最低学历'

            work_types = soup.select('div.terminalpage-left > ul.terminal-ul.clearfix > li:nth-of-type(8) > strong')
            work_type = work_types[0].get_text() if work_types else '未知工作类别'

            company_names = soup.select('body > div.terminalpage.clearfix > div.terminalpage-right > div.company-box > p.company-name-t > a')
            company_name = company_names[0].get_text() if company_names else '未知公司名称'

            company_scales = soup.select('body > div.terminalpage.clearfix > div.terminalpage-right > div.company-box > ul > li:nth-of-type(1) > strong')
            if company_scales:
                company_scale = company_scales[0].get_text()
                pattern = r'([0-9]+)-([0-9]+)'
                # g = re.findall(pattern, month_salary)招聘人数
                g = re.match(pattern, company_scale)
                if g:
                    company_scale = [g.group(1), g.group(2)]
                else:
                    company_scale = '未知公司规模'
            else:
                company_scale = '未发现公司规模'

            company_propertys = soup.select('body > div.terminalpage.clearfix > div.terminalpage-right > div.company-box > ul > li:nth-of-type(2) > strong')
            company_property = company_propertys[0].get_text() if company_propertys else '未知公司性质'

            company_industrys = soup.select('body > div.terminalpage.clearfix > div.terminalpage-right > div.company-box > ul > li:nth-of-type(3) > strong > a')
            company_industry = company_industrys[0].get_text() if company_industrys else '未知公司行业'

            company_addresses = soup.select('body > div.terminalpage.clearfix > div.terminalpage-right > div.company-box > ul > li:nth-of-type(4) > strong')
            company_address = company_addresses[0].get_text() if company_addresses else '未知公司地址'
            company_address = string.strip(company_address[0: company_address.find(r'查')])

            data = {
                'firstLevelName': firstLevelName,
                'secondLevelName': secondLevelName,
                'position_name': position_name,
                'month_salary': month_salary,
                'pub_date': pub_date,
                'work_experience': work_experience,
                'recruit_num': recruit_num,
                'work_place': work_place,
                'full_part_time': full_part_time,
                'work_type': work_type,
                'company_name': company_name,
                'company_scale': company_scale,
                'company_property': company_property,
                'company_industry': company_industry,
                'company_address': company_address,
                'minimum_education': minimum_education
            }
            # print data
            ItemInfo_Table.insert_one(data)
        else:
            print('获取网页数据失败,返回码：' + str(wb_data.status_code))
            return None
    except:
        raise

#通过DetailUrls数据表中的url抓取条目明细,存入数据库中
def get_all_item_info(headers):
    total_item_info_count = 601041
    loaded_count = Detail_Table_tmp.find().count()
    loaded_url = []
    for i in list(Detail_Table_tmp.find({}, {'_id': 0})):
        loaded_url.append(i['href'])
    for item in Detail_Table.find():
        firstLevelName = item['firstLevelName']
        secondLevelName = item['secondLevelName']
        name = item['name']
        href = item['href']
        if href in loaded_url > 0:
            continue
        else:
            # 为防止由于上次抓取异常存在的冗余数据，先做一次删除
            Detail_Table.remove({'href': href})
            try:
                print('正在抓取:==>' + href + '<==的数据........')

                # 每次数据抓取尝试5次
                retry = 0
                while retry < 5:
                    try:
                        get_one_item_info_from(href, firstLevelName, secondLevelName, headers=headers)
                        break
                    except:
                        retry += 1
                        time.sleep(2)
                        continue
                else:
                    print('\033[1;31;40m获取数据' + href + '失败!\033[0m ')
                    continue


                data = {
                    'href': href
                }
                Detail_Table_tmp.insert_one(data)
                loaded_url.append(href)
                print('\033[1;32;40m完成。\033[0m ')
                loaded_count += 1
                print('{loaded}/{total}'.format(loaded=loaded_count, total=total_item_info_count))
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


#迭代器示例
def rev_str(my_str):
    length = len(my_str)
    for i in range(length - 1,-1,-1):
        yield my_str[i]

def PowTwoGen(max = 0):
    n = 0
    while n < max:
        yield 2 ** n
        n += 1


def all_even():
    n = 0
    while True:
        yield n
        n += 2

if __name__ == '__main__':

    # for i in all_even():
    #     print i

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

    #print Detail_Table.count()



    # url = 'http://jobs.zhaopin.com/593598527250540.htm'
    # get_one_item_info_from(url,'销售', '销售代表', headers=jobs_headers)

    get_all_item_info(jobs_headers)
    # print Detail_Table.find().count()  #601041
    # print Detail_Table_tmp.find().count()

