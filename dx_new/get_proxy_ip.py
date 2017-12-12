# -*- coding: utf-8 -*-
from settings import ProxyUrl
import requests
import json


def get_ip_list(ip_url):
    res = requests.get(ip_url)
    res_code = json.loads(res.text)['code']
    while res_code == -51:
        res = requests.get(ip_url)
        res_code = json.loads(res.text)['code']
    return json.loads(res.text)['data']['proxy_list']


def get_proxy_ip_main():
    new_ip = get_ip_list(ProxyUrl)
    return new_ip


if __name__ == '__main__':
    pass













