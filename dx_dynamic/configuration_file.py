# -*- coding: utf-8 -*-


ThreadNum = 30

# redis的key名字设置
DbNum = 12
RedisSpace = 'dx:dynamic:'
NewProduct = '{}{}'.format(RedisSpace, 'new_product')
CrawlProduct = '{}{}'.format(RedisSpace, 'crawl_product')
DynamicProductUrl = '{}{}:'.format(RedisSpace, 'dynamic_product_url')
