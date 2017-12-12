# -*- coding: utf-8 -*-
import requests
from lxml import etree
import random
from store import DxRedis
from settings import HEADERS
import traceback
from requests.exceptions import RequestException
from threading import Thread
import re
import time
from get_proxy_ip import get_proxy_ip_main
from configuration_file import NewPageUrl, CrawlPageUrl, NewProductUrlId, DbNum, ThreadNum, PoaUuid

rds = DxRedis(DbNum)  # 存放集合
rds_12 = DxRedis(12)


class Producer(Thread):
    def __init__(self, que):
        super(Producer, self).__init__()
        self.que = que

    def run(self):
        while True:
            page_url = rds.pop_member(NewPageUrl)
            if page_url is None:
                break
            else:
                if isinstance(page_url, bytes):
                    page_url = page_url.decode('utf-8')
                rds.add_set(CrawlPageUrl, page_url)
                try:
                    if self.que.qsize() < 10:
                        proxy_ip_list = get_proxy_ip_main()
                        for proxy_ip in proxy_ip_list:
                            self.que.put({'ip': proxy_ip, 'num': 0})
                    ip_proxy = self.que.get()
                    ip = ip_proxy['ip']
                    proxy = {"http": 'http://' + ip}
                    header = {"User-Agent": random.choice(HEADERS)}
                    rq = requests.get(page_url, headers=header, proxies=proxy, timeout=5)
                    if rq.status_code == 200:
                        print(rq.url)
                        self.analyze_page_html(rq.text, page_url)
                    else:
                        ip_proxy['num'] += 1
                        print('status_code: %s ' % rq.status_code)
                        rds.add_set(NewPageUrl, page_url)
                except RequestException:
                    ip_proxy['num'] += 1
                    print('REX')
                    #traceback.print_exc()
                    rds.add_set(NewPageUrl, page_url)
                finally:
                    if ip_proxy['num'] <= 5:
                        self.que.put(ip_proxy)
                    else:
                        print('move queue', ip_proxy)

    def analyze_page_html(self, html, url):
        try:
            sel = etree.HTML(html)
            product_lst = sel.xpath('//ul[@class="productList subList"]/li')
            for product in product_lst:
                sku = product.xpath('.//p[@class="sku"]/text()')
                if sku:
                    sku = re.findall(r'\d+', sku[0].strip())
                    if sku:
                        sku = sku[0]
                        poa_id_key = PoaUuid + sku
                        if not rds_12.exists_key(poa_id_key):
                            rds.add_set(NewProductUrlId, sku)

            next_page = sel.xpath('//ul[@class="page"]/li/a[@class="next"]/@href')
            if next_page:
                next_page_url = 'http://www.dx.com' + next_page[0]
                rds.add_set(NewPageUrl, next_page_url)
            rds.remove_member(CrawlPageUrl, url)

        except:
            traceback.print_exc()
            rds.add_set(NewPageUrl, url)


def dx_list_main(que):
    try:
        # 每天新增数据采集
        dt = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        url = 'http://www.dx.com/new-arrivals?date=%s' % dt
        rds.add_set(NewPageUrl, url)

        if rds.exists_key(NewPageUrl) or rds.exists_key(CrawlPageUrl):
            # 程序中止后重启将正在爬取队列里的链接移入待爬队列
            for member in rds.get_all_members(CrawlPageUrl):
                if isinstance(member, bytes):
                    member = member.decode('utf-8')
                rds.add_set(NewPageUrl, member)
            rds.delete_key(CrawlPageUrl)

            url_num = rds.count_members(NewPageUrl)
            thread_num = ThreadNum if url_num > ThreadNum else url_num
            thread_lst = [Producer(que) for i in range(thread_num)]
            for t in thread_lst:
                t.start()
            for t1 in thread_lst:
                t1.join()
    except:
        traceback.print_exc()


if __name__ == '__main__':
    pass
