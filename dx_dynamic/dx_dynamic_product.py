# -*- coding: utf-8 -*-
from lxml import etree
import re
import json
import time
import requests
import random
from store import DxStore, DxRedis
from settings import HEADERS
import traceback
from requests.exceptions import RequestException
from threading import Thread, RLock
from pymysql.err import InterfaceError
from get_proxy_ip import get_proxy_ip_main
from configuration_file import ThreadNum, DbNum, NewProduct, CrawlProduct, DynamicProductUrl

rds = DxRedis(DbNum)
lock = RLock()


class ConsumerThread(Thread):
    def __init__(self, store, que):
        super(ConsumerThread, self).__init__()
        self.store = store()
        self.que = que

    def run(self):
        while True:
            mp = rds.pop_member(NewProduct)
            if mp is None:
                break
            else:
                if isinstance(mp, bytes):
                    mp = mp.decode('utf-8')
                rds.add_set(CrawlProduct, mp)
                if self.que.qsize() < 10:
                    proxy_ip_list = get_proxy_ip_main()
                    for proxy_ip in proxy_ip_list:
                        self.que.put({'ip': proxy_ip, 'num': 0})
                ip_proxy = self.que.get()
                ip = ip_proxy['ip']
                proxy = {"http": 'http://' + ip}
                header = {"User-Agent": random.choice(HEADERS)}
                try:
                    product_url = eval(mp)['product_url']
                    rq = requests.get(product_url, headers=header, proxies=proxy, timeout=5)
                    if rq.status_code == 200:
                        rq.encoding = 'utf-8'
                        lock.acquire()
                        self.parse_html(rq.text, mp, rq.url)
                        lock.release()
                    elif rq.status_code == 404:
                        rds.remove_member(CrawlProduct, mp)
                    else:
                        print(rq.status_code)
                        ip_proxy['num'] += 1
                        rds.add_set(NewProduct, mp)
                except RequestException:
                    print('REX')
                    ip_proxy['num'] += 1
                    rds.add_set(NewProduct, mp)
                finally:
                    if ip_proxy['num'] <= 5:
                        self.que.put(ip_proxy)

    def parse_html(self, html, mpp, p_url):
        try:
            sel = etree.HTML(html)
            category = sel.xpath('//div[@class="position"]/a/text()')
            if len(category) > 0:
                category = ' > '.join(category)
            else:
                category = ''
            category_url = sel.xpath('//div[@class="position"]/a[@class="last"]/@href')
            if len(category_url) > 0:
                category_url = 'http://www.dx.com' + category_url[0].strip()
                category_id = re.findall(r'\d+$', category_url)[0]
            else:
                category_url = ''
                category_id = ''

            #product_url = eval(mpp)['product_url']
            product_url = p_url
            poa_id = sel.xpath('//span[@id="sku"]/text()')
            if len(poa_id) > 0:
                poa_id = poa_id[0].strip()
            else:
                print('no poa %s' % product_url)
                return
            brand = sel.xpath('//td[contains(text(), "Brand")]/following-sibling::*[1]/text()')
            if len(brand) > 0:
                brand = brand[0].strip()
            else:
                brand = ''
            _name = sel.xpath('//span[@id="headline"]/@title')
            if len(_name) > 0:
                _name = _name[0].strip()
            else:
                _name = ''
            first_title = _name
            second_title = sel.xpath('//p[@class="short_tit"]/text()')
            if len(second_title) > 0:
                second_title = second_title[0].strip()
            else:
                second_title = 0
            currency = 'USD'
            currency_id = sel.xpath('//a[@id="currencySymbol"]/span/text()')[0].strip()
            if currency_id == 'US$':
                price = sel.xpath('//span[@id="price"]/text()')
                if len(price) > 0:
                    price = price[0]
                else:
                    price = 0
                original_price = sel.xpath('//del[@id="list-price"]/text()')
                if len(original_price) > 0:
                    original_price = original_price[0]
                    original_price = re.findall(r'\d+\.?\d*', original_price)
                    if len(original_price) > 0:
                        original_price = original_price[0]
                discount_1 = sel.xpath('//span[@id="priceOff"]/b/text()')
                if len(discount_1) > 0:
                    discount = discount_1[0] + '%'
                else:
                    discount = ''
            else:
                price = re.findall(r'google_tag_params = {.+pvalues : \[(.+)\].+}', html, re.S)[0]
                original_price = 0
                discount = ''
            shipping = sel.xpath('//div[starts-with(@class, "shipping")]/p/span[1]/span/text()')
            if len(shipping) > 0:
                shipping = shipping[0].strip()
            else:
                shipping = ''
            dispatch_1 = sel.xpath('//span[contains(text(), "Dispatch")]/../span[2]/text()')
            if len(dispatch_1) > 0:
                dispatch = dispatch_1[0].strip().replace('.', '')
            else:
                dispatch = ''
            status = 0
            review_count = sel.xpath('//span[@itemprop="reviewCount"]/text()')
            if len(review_count) > 0:
                review_count = review_count[0]
            else:
                review_count = 0
            grade_count = sel.xpath('//span[@itemprop="ratingValue"]/text()')
            if len(grade_count) > 0:
                grade_count = grade_count[0]
            else:
                grade_count = 0
            image_url = sel.xpath('//a[@id="product-large-image"]/@href')
            if len(image_url) > 0:
                image_url = 'http:' + image_url[0]
            else:
                image_url = ''
            _extra_image_urls = sel.xpath('//ul[starts-with(@class, "product-small-images")]/li/a/@href')
            extra_image_urls = ','.join(['http:' + eu for eu in _extra_image_urls])
            platform = 'dx'
            platform_url = 'http://www.dx.com/'
            crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            _uuid = eval(mpp)['uuid']
            products_id = _uuid
            attributes = sel.xpath('//li[@id="attrList"]//div')
            if len(attributes) > 0:
                attribute = dict()  # sku总属性
                select_attribute = dict()  # poa属性
                for attr in attributes:
                    att = attr.xpath('span/text()')[0]
                    attribute[att] = attr.xpath('p/a/text()')
                    select_attribute[att] = attr.xpath('p/a[@class="selected"]/text()')
                attribute = json.dumps(attribute)
                select_attribute = json.dumps(select_attribute)
            else:
                attribute = ''
                select_attribute = ''

            if str(eval(mpp)['type']) == 'sku':
                product_attr = re.findall(r'productAttrs: (\[.+\])', html)
                if len(product_attr) > 0:
                    product_attr = product_attr[0]
                    attr_lst = json.loads(product_attr)
                    other_poa_list = [attr['Sku'] for attr in attr_lst if int(attr['Sku']) != int(poa_id)]
                    version_urls_list = ['http://www.dx.com/%s' % other_poa for other_poa in other_poa_list if int(other_poa) != int(poa_id)]
                    version_urls = ','.join(version_urls_list)
                    for poa_url in version_urls_list:
                        new_mpp = {'product_url': poa_url, 'uuid': _uuid, 'type': 'poa'}
                        rds.add_set(NewProduct, new_mpp)
                else:
                    version_urls = ''
            try:
                assert len(_uuid) == 32
            except AssertionError:
                print('uuid length is not 32: %s' % product_url)
                return
            try:
                if str(eval(mpp)['type']) == 'sku':
                    self.store.update_sku(brand, _name, first_title, second_title, original_price, price,
                                          discount,
                                          dispatch, shipping, attribute, version_urls, review_count,
                                          grade_count, image_url, extra_image_urls, category, category_url, category_id,
                                          crawl_time, create_time, 0, product_url, _uuid, platform)   # 0为is_push
                    self.store.insert_dynamic(_uuid, products_id, poa_id, product_url, price, original_price, discount,
                                              select_attribute,
                                              review_count, grade_count, status, platform, crawl_time, create_time)
                if str(eval(mpp)['type']) == 'poa':
                    self.store.insert_dynamic(_uuid, products_id, poa_id, product_url, price, original_price, discount,
                                              select_attribute,
                                              review_count, grade_count, status, platform, crawl_time, create_time)
            except InterfaceError:
                traceback.print_exc()
                self.store.conn.ping()
            except:
                traceback.print_exc()
                rds.add_set(NewProduct, mpp)
            print(product_url)
            rds.remove_member(CrawlProduct, mpp)
            if str(eval(mpp)['type']) == 'sku':
                dynamic_product_url_key = DynamicProductUrl + product_url
                rds.set_key_value(dynamic_product_url_key, 0)
                rds.set_key_exists_time(dynamic_product_url_key, 60 * 60 * 24)
        except:
            rds.add_set(NewProduct, mpp)
            print('product_url', product_url)
            traceback.print_exc()


def dx_product_main(que):
    try:
        if rds.exists_key(NewProduct) or rds.exists_key(CrawlProduct):
            for member in rds.get_all_members(CrawlProduct):
                if isinstance(member, bytes):
                    member = member.decode('utf-8')
                rds.add_set(NewProduct, member)
            rds.delete_key(CrawlProduct)

            product_num = rds.count_members(NewProduct)
            thread_num = ThreadNum if product_num > ThreadNum else product_num
            thread_lst = [ConsumerThread(DxStore, que) for i in range(thread_num)]
            for t in thread_lst:
                t.start()
            for t1 in thread_lst:
                t1.join()
    except:
        traceback.print_exc()


if __name__ == '__main__':
    pass



