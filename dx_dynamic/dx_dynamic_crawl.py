# -*- coding: utf-8 -*-
from dx_dynamic_product import dx_product_main
from get_proxy_ip import get_proxy_ip_main
from store import DxStore, DxRedis
import queue
from configuration_file import DynamicProductUrl, DbNum, NewProduct


def dx_crawl_main(que):
    dx_product_main(que)


def check_if_new():
    dx_store = DxStore()
    rds = DxRedis(DbNum)
    sku_list = dx_store.select_sku_status9()
    for d in sku_list:
        dynamic_product_url_key = DynamicProductUrl + d['scgs_product_url']
        if not rds.exists_key(dynamic_product_url_key):
            mp = {'product_url': d['scgs_product_url'], 'uuid': d['scgs_uuid'], 'type': 'sku'}
            rds.add_set(NewProduct, mp)
    return bool(rds.count_members(NewProduct))


if __name__ == '__main__':
    if check_if_new():
        q = queue.Queue()
        proxy_ip_lst = get_proxy_ip_main()
        for ip in proxy_ip_lst:
            q.put({'ip': ip, 'num': 0})
        dx_crawl_main(q)
    else:
        print('no update')

