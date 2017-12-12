# -*- coding: utf-8 -*-


ThreadNum = 30

# redis的key名字设置
DbNum = 13
RedisSpace = 'dx:site:'
NewPageUrl = '{}{}'.format(RedisSpace, 'new_page_url')
CrawlPageUrl = '{}{}'.format(RedisSpace, 'crawl_page_url')
NewProductUrlId = '{}{}'.format(RedisSpace, 'new_product_url_id')
CrawlProductUrlId = '{}{}'.format(RedisSpace, 'crawl_product_url_id')
PoaId = '{}{}:'.format(RedisSpace, 'poa')
PoaUuid = 'dx:site:poa_id_uuid:'
