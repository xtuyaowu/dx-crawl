# -*- coding: utf-8 -*-
from dx_new_list import dx_list_main
from dx_new_product import dx_product_main
from get_proxy_ip import get_proxy_ip_main
import queue


def dx_crawl_main(que):
    dx_list_main(que)
    dx_product_main(que)


if __name__ == '__main__':
    q = queue.Queue()
    proxy_ip_lst = get_proxy_ip_main()
    for ip in proxy_ip_lst:
        q.put({'ip': ip, 'num': 0})
    dx_crawl_main(q)

