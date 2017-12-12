import requests
from settings import HEADERS
import random
from lxml import etree
from store import DxRedis
from configuration_file import DbNum, NewPageUrl

rds_4 = DxRedis(DbNum)

start_url = "http://www.dx.com/"


def get_page(url, cate=None):
    headers = {"User-Agent": random.choice(HEADERS)}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        resp.encoding = 'utf-8'
        sel = etree.HTML(resp.text)
        item_list_3 = sel.xpath('//div[@class="cate_menu"]/ul/li/a')
        item_list_4 = sel.xpath('//div[@class="menu"]/ul/li/h4/a')[:-3]   # 切片值要根据网站实时情况确定
        if item_list_3:
            item_list = item_list_3
            cls = 2
        elif item_list_4:
            item_list = item_list_4
            cls = 1
        else:
            item_list = ''
        for item in item_list:
            title = item.xpath('./text()')[0].strip()
            title_url = item.xpath('./@href')[0]
            title_url = 'http://www.dx.com' + title_url if not title_url.startswith('http') else title_url
            category = '>'.join([cate, title]) if cate else title
            print(category, title_url, cls)
            mp = {'category_url': title_url, 'cls': cls}
            #rds_4.set_hash(category, mp)
            if cls == 2:
                #mpp = {'category': category, 'page_url': title_url}
                rds_4.add_set(NewPageUrl, title_url)
            if not item_list_3:
                get_page(title_url, category)
        if item_list_3:
            return


if __name__ == '__main__':
    get_page(start_url)
