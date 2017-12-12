import requests
from settings import HEADERS
import random
from lxml import etree
import re

if __name__ == '__main__':
    url = 'http://www.dx.com/c/cell-phones-accessories-599/batteries-503'
    headers = {'User-Agent': random.choice(HEADERS)}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        html = resp.text
        sel = etree.HTML(html)
        product_lst = sel.xpath('//ul[@class="productList subList"]/li')
        for product in product_lst:
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
            print(sku, price, product_url, title)
        next_page = sel.xpath('//ul[@class="page"]/li/a[@class="next"]/@href')
        if next_page:
            next_page_url = 'http://www.dx.com' + next_page[0]
            print(next_page_url)
