# -*- coding: utf-8 -*-
import requests
from lxml import etree
import random
from store import DxRedis, DxStore
from settings import HEADERS
import traceback
from requests.exceptions import RequestException
from threading import Thread
import re
import time
from get_proxy_ip import get_proxy_ip_main
from configuration_file import (
    DbNum, NewPageUrl, CrawlPageUrl, RedisSpace, COUNT, NewProductUrlId, SkuSet, PoaUuid, ThreadNum)

rds = DxRedis(DbNum)  # 存放集合
rds_12 = DxRedis(12)


class Producer(Thread):
    def __init__(self, store, que):
        super(Producer, self).__init__()
        self.store = store()
        self.que = que

    def run(self):
        while True:
            mp = rds.pop_member(NewPageUrl)
            if mp is None:
                break
            else:
                if isinstance(mp, bytes):
                    mp = mp.decode('utf-8')
                rds.add_set(CrawlPageUrl, mp)
                mpp = eval(mp)
                try:
                    if self.que.qsize() < 10:
                        proxy_ip_list = get_proxy_ip_main()
                        for proxy_ip in proxy_ip_list:
                            self.que.put({'ip': proxy_ip, 'num': 0})
                    ip_proxy = self.que.get()
                    ip = ip_proxy['ip']
                    proxy = {"http": 'http://' + ip}
                    header = {"User-Agent": random.choice(HEADERS)}
                    page_url = mpp['page_url']
                    rq = requests.get(page_url, headers=header, proxies=proxy, timeout=5)
                    if rq.status_code == 200:
                        print(rq.url)
                        self.analyze_page_html(rq.text, mp)
                    else:
                        ip_proxy['num'] += 1
                        print('status_code: %s ' % rq.status_code)
                        rds.add_set(NewPageUrl, mp)
                except RequestException:
                    ip_proxy['num'] += 1
                    print('REX')
                    #traceback.print_exc()
                    rds.add_set(NewPageUrl, mp)
                finally:
                    if ip_proxy['num'] <= 5:
                        self.que.put(ip_proxy)
                    else:
                        print('move queue', ip_proxy)

    def analyze_page_html(self, html, mp):
        try:
            mpp = eval(mp)
            category = mpp['category']
            amount = mpp.get('amount', COUNT)
            category_url = mpp.get('category_url', None)
            if not category_url:
                category_url = mpp['page_url']
                mpp['category_url'] = category_url
            category_id = re.findall(r'\d+$', category_url)[0]
            category_url_key = RedisSpace + category_url
            sel = etree.HTML(html)
            currency = sel.xpath('//span[contains(@class, "currency-span")]/text()')
            if currency:
                currency = currency[0].strip()
                print(currency)
                if currency != 'USD':
                    rds.add_set(NewPageUrl, mp)
                    return
            product_lst = sel.xpath('//ul[@class="productList subList"]/li')
            for product in product_lst:
                count = rds.count_members(category_url_key)
                if COUNT and count >= amount:
                    rds.delete_key(category_url_key)
                    break
                sku = product.xpath('.//p[@class="sku"]/text()')
                if sku:
                    sku = re.findall(r'\d+', sku[0].strip())
                    if sku:
                        sku = sku[0]
                    else:
                        sku = ''
                else:
                    sku = ''
                title = product.xpath('.//p[@class="title"]/a/@title')
                if title:
                    title = title[0].strip()
                else:
                    title = ''
                product_url = product.xpath('.//p[@class="title"]/a/@href')
                if product_url:
                    product_url = '{}{}'.format('http://www.dx.com', product_url[0].strip())
                else:
                    product_url = '{}{}{}'.format('http://www.dx.com', '/', sku)
                price = product.xpath('.//p[@class="price"]/text()')
                if price:
                    price = re.findall(r'\d+\.?\d*', price[0].strip())
                    if price:
                        price = price[0]
                    else:
                        price = 0
                else:
                    price = 0
                if sku:
                    if rds.is_member(SkuSet, sku) or rds.is_member(category_url, sku):
                        continue
                    rds.add_set(SkuSet, sku)
                    rds.add_set(category_url_key, sku)
                    rank = rds.count_members(category_url_key)
                    key = PoaUuid + sku
                    if rds_12.exists_key(key):
                        _uuid = rds_12.get_hash_field(key, 'uuid')
                        if isinstance(_uuid, bytes):
                            _uuid = _uuid.decode('utf-8')
                        status = 0
                    else:
                        rds.add_set(NewProductUrlId, sku)
                        _uuid = sku
                        status = -1
                    crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    self.store.insert_global_track(_uuid, sku, product_url, title, price, category, category_url,
                                                   category_id, 'popularity', rank, 'dx', 'dx.com', crawl_time,
                                                   create_time, status)
            else:
                next_page = sel.xpath('//ul[@class="page"]/li/a[@class="next"]/@href')
                if next_page:
                    next_page_url = 'http://www.dx.com' + next_page[0]
                    mpp['page_url'] = next_page_url
                    rds.add_set(NewPageUrl, mpp)
                else:
                    rds.delete_key(category_url_key)
            rds.remove_member(CrawlPageUrl, mp)

        except:
            traceback.print_exc()
            rds.add_set(NewPageUrl, mp)


def dx_list_main(que):
    try:
        if rds.exists_key(NewPageUrl) or rds.exists_key(CrawlPageUrl):
            # 程序中止后重启将正在爬取队列里的链接移入待爬队列
            for member in rds.get_all_members(CrawlPageUrl):
                if isinstance(member, bytes):
                    member = member.decode('utf-8')
                rds.add_set(NewPageUrl, member)
            rds.delete_key(CrawlPageUrl)

            url_num = rds.count_members(NewPageUrl)
            thread_num = ThreadNum if url_num > ThreadNum else url_num
            thread_lst = [Producer(DxStore, que) for i in range(thread_num)]
            for t in thread_lst:
                t.start()
            for t1 in thread_lst:
                t1.join()
            rds.delete_key(SkuSet)
    except:
        traceback.print_exc()


if __name__ == '__main__':
    pass
