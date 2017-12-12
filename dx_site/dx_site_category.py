import requests
from settings import HEADERS
import random
from lxml import etree
from store import DxRedis

rds_4 = DxRedis(16)
start_url = "http://www.dx.com/"


def get_page(url, cate=None):
    headers = {"User-Agent": random.choice(HEADERS)}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        resp.encoding = 'utf-8'
        sel = etree.HTML(resp.text)
        item_list_1 = sel.xpath('//div[@class="filterCategory"]/ul/li[contains(@class, "level3")]/a')
        item_list_2 = sel.xpath('//div[@class="filterCategory"]/ul/li[contains(@class, "level2")]/strong')
        item_list_3 = sel.xpath('//div[@class="cate_menu"]/ul/li/a')
        item_list_4 = sel.xpath('//div[@class="menu"]/ul/li/h4/a')[:-3]   # 切片值要根据网站实时情况确定
        if item_list_1:
            item_list = item_list_1
            minimum = 1
        elif item_list_2:
            item_list = item_list_2
            minimum = 1
        elif item_list_3:
            item_list = item_list_3
            minimum = 0
        elif item_list_4:
            item_list = item_list_4
            minimum = 0
        else:
            item_list = ''
        for item in item_list:
            if (not item_list_1) and item_list_2:
                title_url = resp.url
                category = cate
            else:
                title = item.xpath('./text()')[0].strip()
                title_url = item.xpath('./@href')[0]
                title_url = 'http://www.dx.com' + title_url if not title_url.startswith('http') else title_url
                category = '>'.join([cate, title]) if cate else title
            print(category, title_url, minimum)
            mp = {'category_url': title_url, 'minimum': minimum}
            rds_4.set_hash(category, mp)
            if not (item_list_1 or item_list_2):
                get_page(title_url, category)
        if item_list_1 or item_list_2:
            return


if __name__ == '__main__':
    get_page(start_url)
