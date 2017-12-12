# -*- coding: utf-8 -*-
from dx_popular_list import dx_list_main
from dx_popular_product import dx_product_main
from dx_popular_update_track_uuid import update_track
from dx_popular_get_category2_url import get_page
from get_proxy_ip import get_proxy_ip_main
import queue
from configuration_file import DbNum, NewPageUrl
from store import DxRedis

rds = DxRedis(DbNum)
start_url = "http://www.dx.com/"


def dx_crawl_main(que):
    #get_page(start_url)
    for mem in rds.get_all_members('category2_url'):
        rds.add_set(NewPageUrl, mem)
    print('get_category2_url finished')
    dx_list_main(que)
    print('list finished')
    dx_product_main(que)
    print('product finished')
    update_track()
    print('update_track finished')


if __name__ == '__main__':
    q = queue.Queue()
    proxy_ip_lst = get_proxy_ip_main()
    for ip in proxy_ip_lst:
        q.put({'ip': ip, 'num': 0})
    dx_crawl_main(q)
