# -*- coding: utf-8 -*-
# 此代码仅用于学习交流使用，严禁用于非法用途
from DrissionPage._pages.web_page import WebPage
from curl_cffi import requests
import json,ssl,time,math,warnings,os,re
from urllib.parse import unquote,urljoin
from pyquery import PyQuery as pq
import pandas as pd


ssl._create_default_https_context = ssl._create_unverified_context
warnings.filterwarnings("ignore")

import sqlite3


def conn_db(db_file):
    # 创建连接
    conn = sqlite3.connect(db_file)
    # 创建游标
    return conn, conn.cursor()


def exec_sql(conn, cur, sql, data=()):
    if type(data) == list:
        cur.executemany(sql, data)
    else:
        cur.execute(sql, data)
    conn.commit()
    return cur


def close_conn(conn, cur):
    # 提交更改并关闭连接
    cur.close()
    conn.close()



class Record:
    def __init__(self):
        self.title = ''
        self.author = ''
        self.pub_date = ''
        self.content = ''
        self.link = ''
        self.category = ''

    def addRecord(self, db_file):
        data = (self.title, self.author, self.pub_date, self.content, self.link,self.category)
        conn, cur = conn_db(db_file)
        sql = '''
        insert into `record`(
            `title`,`author`,`pub_date`,`content`,`link`,`category`
        ) values(
            ?,?,?,?,?,?
        )
        '''
        try:
            exec_sql(conn, cur, sql, data)
        except:
            pass
        close_conn(conn, cur)

    def checkExists(self, db_file):
        conn, cur = conn_db(db_file)
        sql = 'select * from record where link=?'
        req = exec_sql(conn, cur, sql, (self.link,))
        res = req.fetchone()
        close_conn(conn, cur)
        return True if res else False



class DzSpider(object):
    def __init__(self):
        # 过程数据缓存
        self.data = {}
        # 数据存储目录
        self.folder = fr'{os.getcwd()}'
        # 数据库
        self.db_file = fr'{self.folder}\data.db'
        # 开始页码
        self.start_page = 1
        # 结束页码
        self.end_page = 10
        # 采集数量标识
        self.spider_num = 1
        # 页码大小
        self.page_size = 20
        # 是否已结束
        self.has_finish = False
        # 是否自动获取页码
        self.reset_end_page = True
        # 请求头信息
        self.headers_str = '''
        accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
		accept-language: zh-CN,zh;q=0.9,en;q=0.8
		cache-control: no-cache
		pragma: no-cache
		priority: u=0, i
		referer: https://www.economist.com/topics/business
		sec-ch-ua: "Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"
		sec-ch-ua-mobile: ?0
		sec-ch-ua-platform: "Windows"
		sec-fetch-dest: document
		sec-fetch-mode: navigate
		sec-fetch-site: same-origin
		sec-fetch-user: ?1
		upgrade-insecure-requests: 1
		user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36
		Cookie: economist_has_visited_app_before=true; user_geo_country=AU; _cfuvid=mL9AuqFLg3JsauJRe6ktnHR.GAdju3wircUUlFMKELo-1764221046027-0.0.1.1-604800000; optimizelyEndUserId=oeu1764221062037r0.7368648897332045; _parsely_session={%22sid%22:1%2C%22surl%22:%22https://www.economist.com/topics/economy?after=1574b006-43bc-46be-abf3-bbb9ab091a4c%22%2C%22sref%22:%22%22%2C%22sts%22:1764221062635%2C%22slts%22:0}; _parsely_visitor={%22id%22:%22pid=1266b8e5-75c0-4263-972f-c6b9f82dcce7%22%2C%22session_count%22:1%2C%22last_session_ts%22:1764221062635}; utag_main__sn=1; utag_main_ses_id=1764221063955%3Bexp-session; initialReferrer=; landingURL=https://www.economist.com/topics/economy?after=1574b006-43bc-46be-abf3-bbb9ab091a4c; landingScreen=topic.economy; blaize_session=eb03facb-3272-4fb8-8dd6-5ad022516ed5; blaize_tracking_id=dcd9b783-f878-4ab2-bbb3-e6f6d959d1b9; utag_main__ss=0%3Bexp-session; utag_main_v_id=019ac3c5153a000bae947677b2650506f005306700bd0; utag_main_dc_visit=1; _sp_su=false; _consentT=true; utag_main_dc_region=ap-southeast-2%3Bexp-session; _fbp=fb.1.1764221065542.938105214662584078; permutive-id=ae77805f-b82a-4f92-b988-20b58f766cf4; _gcl_au=1.1.1321231804.1764221066; __spdt=271029a9c69a4175a7a3a2b4006dea63; _mibhv=anon-1764221066627-5540366452_9933; _li_dcdm_c=.economist.com; _lc2_fpi=6b85dc1d2bba--01kb1wa7e0bvxxx5dh36re2vj6; _lc2_fpi_js=6b85dc1d2bba--01kb1wa7e0bvxxx5dh36re2vj6; wall_session=eb03facb-3272-4fb8-8dd6-5ad022516ed5; __ps_r=_; __ps_sr=_; __ps_lu=https://www.economist.com/topics/economy?after=1574b006-43bc-46be-abf3-bbb9ab091a4c; __ps_slu=https://www.economist.com/topics/economy?after=1574b006-43bc-46be-abf3-bbb9ab091a4c; __ps_fva=1764221066856; tfpsi=b833821d-fcd0-4d90-83c5-18adec4244a3; _hjSession_3355938=eyJpZCI6ImQyODc3ZTBhLTBjZTgtNDVlNi05YWFkLTM3ZjkxZjdiMTc5YiIsImMiOjE3NjQyMjEwNjc3NzQsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MX0=; BCSessionID=b862ed39-6b5e-4aab-8b40-d24f96d1e6f7; _hjSessionUser_3355938=eyJpZCI6IjUzOTYwM2ZjLTYwNjItNTJmYS1hZGNiLTMwMTdlYjA0OTVmNyIsImNyZWF0ZWQiOjE3NjQyMjEwNjc3NzMsImV4aXN0aW5nIjp0cnVlfQ==; _tt_enable_cookie=1; _ttp=01KB1WEWYJCBQBJ08G9V3TYRC4_.tt.1; _gid=GA1.2.610941007.1764221254; economist_auth_complete=1764221672; fcx_contact_id=003WT00000O2RO6YAN; economist_piano_id=003WT00000O2RO6YAN; fcx_access_token=00D3z000002Jvyi\u0021AQEAQFVtHFhoLWjuIx08AztEK7bDVXU6UvlEPhl7eBMHBjsFFh4ZlEAPoWlqDUEE1efjQ3_yGsXiy7mqb8KykdTKxTsOpyMH; fcx_refresh_token=5Aep861ikeX7i6YENc9Sn4e4tYFRWdbjVaKAcXUBkjzxEH.WJ1HMtCYUZyiBuJrnlt1.VeIO2fzab3VJOEKMNjk; state-is-subscriber=false; fcx_auth_type=fcx; cf_clearance=o9LduDYZVw.RANYvJqMYBdi1SVI0stnW6rdSQMrNO8k-1764221814-1.2.1.1-DXvMvVnl.ZNRTW_._JzyE5nRXoJ3AMrNA8n2CzrWynZbr2Jfcbra7mQZMbjrcrva6bDErMsxU4b8uNVLlmrMIZYjrTCLpIsJXQAtqLMWvnVdgvOVRCc9KM5QJ7XWyVIEtyXBy3cQ0lrLL.1X76OX_GuGh0GLqDQ1W1hQVWkHycdEDYHyO.UV935JrdLLD4c8mvWODIE55UPyrp00pTl7TZdOXGZR9rNT2l8hzvnRbiU; subscribe_cta=article%7Csubscribe%3Aarticle.paywall; state-75d2a3572c1c8cf41c646e583dec4b27=%7B%22returnUrl%22%3A%22https%3A%2F%2Fwww.economist.com%2Fthe-world-ahead%2F2025%2F11%2F12%2Fthe-british-economy-will-pick-up-a-little%22%7D; fcx_access_state=eyJvd25lckFjY291bnRJZCI6IjAwMVdUMDAwMDJLZWY2U1lBUiIsInN1YnNjcmlwdGlvblR5cGUiOiJCMkMiLCJjb250YWN0SWQiOiIwMDNXVDAwMDAwTzJSTzZZQU4iLCJlbWFpbCI6InNoZW5neWl5dV8wNTJAYmVya2VsZXkuZWR1IiwiZmlyc3ROYW1lIjoiU2hlbmd5aSIsImxhc3ROYW1lIjoiWXUiLCJwaG9uZSI6bnVsbCwiaXNFbWFpbFZlcmlmaWVkIjoiZmFsc2UiLCJpc0IyYkNsaWVudEFkbWluIjoiZmFsc2UiLCJpc1N1YnNjcmliZXIiOnRydWUsImlzTGFwc2VkIjpmYWxzZSwibGFwc2VkTW9udGhzIjowLCJjdXN0b21lclNlZ21lbnQiOm51bGwsImxvZ2dlZEluIjp0cnVlLCJyZWdpc3RlcmVkQXQiOiIyMDI1LTEwLTA3VDIyOjQzOjUyLjAwMCswMDAwIiwiaXNFc3ByZXNzb1N1YnNjcmliZXIiOmZhbHNlLCJpc1BvZGNhc3RTdWJzY3JpYmVyIjpmYWxzZSwiZW50aXRsZW1lbnRzIjpbeyJwcm9kdWN0Q29kZSI6IlRFLkRJR0lUQUwiLCJlbnRpdGxlbWVudENvZGUiOlt7ImNvZGUiOiJURS5BUFAiLCJleHBpcmVzIjoiMjAyNS0xMi0wNFQyMzo1OTo1OS4wMDBaIn0seyJjb2RlIjoiVEUuV0VCIiwiZXhwaXJlcyI6IjIwMjUtMTItMDRUMjM6NTk6NTkuMDAwWiJ9LHsiY29kZSI6IlRFLk5FV1NMRVRURVIiLCJleHBpcmVzIjoiMjAyNS0xMi0wNFQyMzo1OTo1OS4wMDBaIn0seyJjb2RlIjoiVEUuUE9EQ0FTVCIsImV4cGlyZXMiOiIyMDI1LTEyLTA0VDIzOjU5OjU5LjAwMFoifSx7ImNvZGUiOiJURS5JTlNJREVSTkwiLCJleHBpcmVzIjoiMjAyNS0xMi0wNFQyMzo1OTo1OS4wMDBaIn0seyJjb2RlIjoiVEUuSU5TSURFUiIsImV4cGlyZXMiOiIyMDI1LTEyLTA0VDIzOjU5OjU5LjAwMFoifV19XSwic3Vic2NyaXB0aW9ucyI6W10sImxvZ29VcmwiOm51bGwsImxhcmdlTG9nb1VybCI6bnVsbCwic2hvd1dlbGNvbWVNb2RhbExvZ28iOiJ0cnVlIiwic2hvd09yZ2FuaXNhdGlvbkxvZ28iOiJmYWxzZSJ9; __cf_bm=yg5I80VjfqGQQL_PrTVghERLpxZGT206dVnwXVnGroE-1764222085-1.0.1.1-7N1KtRUlPU17xJT7xPHH9xbtDHZuFfnCMbWMP5jd9_cHra78_xvxHjVh.yuVfGOYSp56JYBJA.jPGZTIG3Hyxk3TyRh0CrY6v9AG3wPh7cM; fcx_user=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIwMDNXVDAwMDAwTzJSTzZZQU4iLCJlbnRpdGxlbWVudHMiOlsiVEUuQVBQIiwiVEUuV0VCIiwiVEUuTkVXU0xFVFRFUiIsIlRFLlBPRENBU1QiLCJURS5JTlNJREVSTkwiLCJURS5JTlNJREVSIl0sInVzZXJUeXBlIjoic3Vic2NyaWJlciIsImV4cCI6MTc3MjAwMTczNiwicmVnaXN0cmF0aW9uVGltZXN0YW1wIjoxNzU5ODc3MDMyLCJzdWJzY3JpcHRpb25UaW1lc3RhbXAiOjE3NjIzMDA3OTksImlhdCI6MTc2NDIyMjEzNn0.EJHJQl1eXw66t_T14ZUCWrClUyBis5jWlyRbukAPWqk; utag_topics_content_id=; utag_topics_id=; utag_topics_label=; utag_topics_probability=; utag_main__pn=16%3Bexp-session; uspv=1; utag_main_dc_event=19%3Bexp-session; last_visit_bc=1764222157381; _rdt_uuid=1764221066622.a04fb347-1d1d-4553-877a-2268ba6101fb; _uetsid=56e29ea0cb5111f082b4efbf09715c0c; _uetvid=56e28970cb5111f097b99350abf712bd; utag_main__se=143%3Bexp-session; utag_main__st=1764223958250%3Bexp-session; _ga_WL00EXBGPS=GS2.1.s1764221244$o1$g1$t1764222158$j3$l0$h0; _ga_CWJ8XWVBYS=GS2.1.s1764221065$o1$g1$t1764222158$j54$l0$h0; ttcsid=1764221219799::p6ApcEbLCVrHHI6g2aLp.1.1764222158346.0; ttcsid_C9V6D2JC77UE268F335G=1764221219798::-rdO_pRTJl5MsKlboTOt.1.1764222158346.0; _ga=GA1.2.1198159913.1764221066; __gads=ID=9d92b75034a6fc88:T=1764221051:RT=1764222173:S=ALNI_MaDkfd59xypICi1pUy_raNzQawjEA; __gpi=UID=000011bd8d424b1f:T=1764221051:RT=1764222173:S=ALNI_MalKLyT9bDDPFrozBNT1uBpSqMrZQ; __eoi=ID=46ed2c2f6da6fe5c:T=1764221051:RT=1764222173:S=AA-AfjYgzUqfRR-Bz0vqfP0qD1ya; _parsely_slot_click={%22url%22:%22https://www.economist.com/topics/business%22%2C%22x%22:838%2C%22y%22:131%2C%22xpath%22:%22//*[@id=%5C%22__next%5C%22]/div[1]/div[1]/div[2]/header[1]/div[1]/nav[1]/div[1]/ul[1]/li[7]/a[1]%22%2C%22href%22:%22https://www.economist.com/topics/economy%22}
        '''
        self.headers = dict(
            [[y.strip() for y in x.strip().split(':', 1)] for x in self.headers_str.strip().split('\n') if x.strip()])
    
    def run_task(self):
        # 遍历所有的页码，采集数据
        req_url='https://www.economist.com/topics/economy'
        # req_url='https://www.economist.com/topics/business'
        self.data['category'] = req_url.split('/')[-1]
        page = WebPage('d')
        page.get(req_url)
        html = page.html
        doc = pq(html)
        after= doc('a[data-analytics="topic_page:next"]').attr('href')
        print(after)

        for a in doc('h3[class*="headline_mb-teaser__headline"]').items():
            title = a.text()
            link = a('a').attr('href')
            if link:
                link = urljoin(req_url,link)
                print(title,link)
                record = Record()
                record.link = link
                record.category = self.data.get('category')
                if record.checkExists(self.db_file):
                    print('已存在')
                    continue
                res = self.get_detail(page,record)
                if res is None:
                    break
        page_index = 2
        while not self.has_finish:
            if after:
                page.get(after)
                html = page.html
                doc = pq(html)
                after = doc('a[data-analytics="topic_page:next"]').attr('href')
                print(after)
                for a in doc('h3[class*="headline_mb-teaser__headline"]').items():
                    title = a.text()
                    link = a('a').attr('href')
                    if link:
                        link = urljoin(req_url, link)
                        print(page_index,title, link)
                        record = Record()
                        record.link = link
                        record.category = self.data.get('category')
                        if record.checkExists(self.db_file):
                            continue
                        res = self.get_detail(page, record)
                        if res is None:
                            print('时间过了')
                            break
                page_index += 1
            else:
                print('没下一页了')
                break

    
    def get_detail(self,page,record):
        link = record.link
        page.new_tab(link)
        t = page.get_tab(page.tab_ids[0])
        html = t.html
        doc = pq(html)

        one_data= {
            'title':doc('h1').text(),
            'pub_date':doc('time').attr('datetime'),
            'content':doc('.css-19tnosi').text()
        }
        print(one_data)
        record.title = one_data.get('title')
        record.pub_date = one_data.get('pub_date')
        try:
            if int(record.pub_date[:4])<2015:
                self.has_finish = True
                return None
        except:
            return True
        record.content = one_data.get('content')
        if not record.checkExists(self.db_file):
            record.addRecord(self.db_file)
        page.close_tabs(page.tab_ids[0])
        return True


if __name__ == '__main__':
    spider = DzSpider()
    spider.run_task()               
                