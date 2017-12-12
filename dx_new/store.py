# -*- coding: utf-8 -*-
from settings import MYSQL_CONFIG_LOCAL, MYSQL_CONFIG_SERVER
import sys
import traceback
import pymysql
from pymysql.err import InterfaceError
import redis


class DxRedis:
    def __init__(self, db):
        try:
            #self.rds = redis.StrictRedis(host='localhost', port=6379, db=db)
            self.rds = redis.StrictRedis(host='localhost', port=6361, db=db)
        except:
            traceback.print_exc()

    def count_keys(self):  # 查询当前库里有多少key
        rds = self.rds
        return rds.dbsize()

    # key操作
    def get_key(self):  # 没有返回None
        rds = self.rds
        return rds.randomkey()

    def exists_key(self, key):
        rds = self.rds
        return rds.exists(key)

    def delete_key(self, key):
        rds = self.rds
        rds.delete(key)

    def get_all_keys(self):
        rds = self.rds
        return rds.keys()

    def move_keys(self, key, num):   # 如果移入的库中已有此key，则无法移入
        rds = self.rds
        rds.move(key, num)

    def set_key_exists_time(self, key, seconds):   # 给已存在的key设置存在时间（秒），超时被redis删除
        rds = self.rds
        rds.expire(key, seconds)

    # String操作
    def set_key_value(self, key, value):
        rds = self.rds
        rds.set(key, value)

    def get_key_value(self, key):   # 没有对应key返回None
        rds = self.rds
        return rds.get(key)

    # Hash操作
    def set_hash(self, key, mapping):   # mapping为字典, 已存在key会覆盖mapping
        rds = self.rds
        rds.hmset(key, mapping)

    def delete_hash_field(self, key, field):   # 删除hash表中某个字段，无论字段是否存在
        rds = self.rds
        rds.hdel(key, field)

    def exists_hash_field(self, key, field):   # 检查hash表中某个字段存在
        rds = self.rds
        return rds.hexists(key, field)

    def get_hash_field(self, key, field):   # 获取hash表中指定字段的值, 没有返回None
        rds = self.rds
        return rds.hget(key, field)

    def get_hash_all_field(self, key):   # 获取hash表中指定key所有字段和值,以字典形式，没有key返回空字典
        rds = self.rds
        return rds.hgetall(key)

    def increase_hash_field(self, key, field, increment):   # 为hash表key某个字段的整数型值增加increment
        rds = self.rds
        rds.hincrby(key, field, increment)

    # List操作
    def push_into_lst(self, key, value):  # url从左至右入列
        rds = self.rds
        rds.rpush(key, value)

    def pop_lst_item(self, key):  # 从左至右取出列表第一个元素(元组形式)，并设置超时，超时返回None
        rds = self.rds
        return rds.blpop(key, timeout=5)

    # Set操作
    def add_set(self, key, value):
        rds = self.rds
        rds.sadd(key, value)

    def is_member(self, key, value):
        rds = self.rds
        return rds.sismember(key, value)

    def pop_member(self, key):  # 随机移除一个值并返回该值,没有返回None
        rds = self.rds
        return rds.spop(key)

    def pop_members(self, key, num):  # 随机取出num个值（非移除），列表形式返回这些值，没有返回空列表
        rds = self.rds
        return rds.srandmember(key, num)

    def remove_member(self, key, value):   # 移除集合中指定元素
        rds = self.rds
        rds.srem(key, value)

    def get_all_members(self, key):   # 返回集合中全部元素,不删除
        rds = self.rds
        return rds.smembers(key)

    def remove_into(self, key1, key2, value):   # 把集合key1中value元素移入集合key2中
        rds = self.rds
        rds.smove(key1, key2, value)

    def count_members(self, key):   # 计算集合中成员数量
        rds = self.rds
        return rds.scard(key)


class DxStore:
    def __init__(self):
        try:
            #self.conn = pymysql.connect(**MYSQL_CONFIG_LOCAL)
            self.conn = pymysql.connect(**MYSQL_CONFIG_SERVER)
        except InterfaceError:
            self.conn.ping()

    # dx_category
    def insert_category(self, _name, code, parent_code, structure, rank, if_minimum, status, is_delete, platform, create_time ):
        sql = "insert into scb_crawler_global_category(scgc_name,scgc_code,scgc_parent_code,scgc_structure,scgc_rank," \
              "scgc_if_minimum,scgc_status,scgc_is_delete,scgc_platform,scgc_create_time)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cur = self.conn.cursor()
        cur.execute(sql, (_name, code, parent_code, structure, rank, if_minimum, status, is_delete, platform, create_time))
        self.conn.commit()

    # dx_list_url
    def select_minimum_category(self, category, domain):
        sql = "select scgc_code, scgc_structure from scb_crawler_global_category where scgc_structure like '%s'" \
              "and scgc_if_minimum=1 and scgc_status=0 and scgc_platform='%s' limit 1" % (category, domain)
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchone()

    def select_all_minimum_category(self, domain):
        sql = 'select scgc_code, scgc_structure from scb_crawler_global_category where scgc_if_minimum=1 AND ' \
              'scgc_status=0 and scgc_platform="%s"' % domain

        cur = self.conn.cursor()
        result = cur.execute(sql)
        return cur.fetchmany(result)

    def set_minimum_category_status_1(self, code):
        sql = "update scb_crawler_global_category set scgc_status=1 where scgc_code='%s'" % code
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    # dx_product
    def insert_sku(self, _uuid, products_id, url_id, brand, product_url, _name, first_title, second_title, original_price,
                            price, discount, dispatch, shipping, currency, attribute, version_urls, review_count, grade_count,
                            image_url, extra_image_urls, category, category_url, category_id, platform, platform_url, status, crawl_time, create_time,
                            description='', tags='', shop_url=''):
        sql = "insert into scb_crawler_global_sku(scgs_uuid, scgs_products_id, scgs_url_id, scgs_brand, scgs_product_url, scgs_name," \
              "scgs_firstTitle, scgs_secondTitle, scgs_original_price, scgs_price, scgs_discount, scgs_dispatch, scgs_shipping," \
              "scgs_currency, scgs_attribute, scgs_version_urls, scgs_review_count, scgs_grade_count, scgs_image_url, scgs_extra_image_urls," \
              "scgs_category, scgs_category_url, scgs_category_id, scgs_platform, scgs_platform_url, scgs_status, scgs_crawl_time, scgs_create_time, scgs_description," \
              "scgs_tags, scgs_shop_url)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur = self.conn.cursor()
        cur.execute(sql, (_uuid, products_id, url_id, brand, product_url, _name, first_title, second_title, original_price,
                          price, discount, dispatch, shipping, currency, attribute, version_urls, review_count, grade_count,
                          image_url, extra_image_urls, category, category_url, category_id, platform, platform_url, status, crawl_time, create_time,
                          description, tags, shop_url))
        self.conn.commit()

    def update_sku(self, brand, _name, first_title, second_title, original_price, price, discount, dispatch, shipping,
                   attribute, version_urls, review_count, grade_count, image_url, extra_image_urls, category,
                   category_url, category_id, crawl_time, create_time, is_push, url, _uuid, platform):
        sql = "update scb_crawler_global_sku set scgs_brand=%s, scgs_name=%s," \
              "scgs_firstTitle=%s, scgs_secondTitle=%s, scgs_original_price=%s, scgs_price=%s, scgs_discount=%s, scgs_dispatch=%s, scgs_shipping=%s," \
              "scgs_attribute=%s, scgs_version_urls=%s, scgs_review_count=%s, scgs_grade_count=%s, scgs_image_url=%s, scgs_extra_image_urls=%s," \
              "scgs_category=%s, scgs_category_url=%s, scgs_category_id=%s, scgs_crawl_time=%s, scgs_create_time=%s, scgs_is_push=%s, scgs_product_url=%s where scgs_uuid=%s and scgs_platform=%s"

        cur = self.conn.cursor()
        cur.execute(sql, (brand, _name, first_title, second_title, original_price, price, discount, dispatch, shipping,
                          attribute, version_urls, review_count, grade_count, image_url, extra_image_urls, category,
                          category_url, category_id, crawl_time, create_time, is_push, url, _uuid, platform))
        self.conn.commit()

    def update_sku_url(self, url, _uuid, platform):
        sql = "update scb_crawler_global_sku set scgs_product_url=%s where scgs_uuid=%s and scgs_platform=%s"
        cur = self.conn.cursor()
        cur.execute(sql, (url, _uuid, platform))
        self.conn.commit()

    def update_poa_url(self, url, _uuid, platform):
        sql = "update scb_crawler_global_poa set scgp_url=%s where scgs_uuid=%s and scgp_platform=%s"
        cur = self.conn.cursor()
        cur.execute(sql, (url, _uuid, platform))
        self.conn.commit()

    def insert_poa(self, _uuid, product_id, poa_id, url, price, original_price, discount, attribute, review_count, grade_count,
                   status, platform, crawl_time, create_time):
        sql = "insert into scb_crawler_global_poa(scgs_uuid, scgs_product_id, scgp_product_id, scgp_url, scgp_price, scgp_original_price," \
              "scgp_discount, scgp_attribute, scgp_review_count, scgp_grade_count, scgp_status, scgp_platform, scgp_crawl_time, scgp_create_time)" \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cur = self.conn.cursor()
        cur.execute(sql, (_uuid, product_id, poa_id, url, price, original_price, discount, attribute, review_count, grade_count,
                          status, platform, crawl_time, create_time))
        self.conn.commit()

    def insert_dynamic(self, _uuid, product_id, d_product_id, url, price, original_price, discount, attribute, review_count, grade_count,
                       status, platform, crawl_time, create_time):
        sql = "insert into scb_crawler_global_sku_dynamic(scgs_uuid, scgs_product_id, scgsd_product_id, scgsd_url, scgsd_price, scgsd_original_price," \
              "scgsd_discount, scgsd_attribute, scgsd_review_count, scgsd_grade_count, scgsd_status, scgsd_platform, scgsd_crawl_time, scgsd_create_time)" \
              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cur = self.conn.cursor()
        cur.execute(sql, (_uuid, product_id, d_product_id, url, price, original_price, discount, attribute, review_count, grade_count,
                          status, platform, crawl_time, create_time))
        self.conn.commit()

    def insert_global_track(self, _uuid, products_id, product_url, _name, price, category, category_url, category_id,
                            track_type, rank, platform, platform_url, crawl_time, create_time, status):
        sql = (
            "insert into scb_crawler_global_track(scgs_uuid, scgs_products_id, scgs_product_url, scgst_name,"
            "scgst_price, scgst_category, scgst_category_url, scgst_category_id, scgst_track_type, scgst_rank,"
            "scgst_platform, scgst_platform_url, scgst_crawl_time, scgst_create_time, scgst_status)"
            "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (_uuid, products_id, product_url, _name, price, category, category_url, category_id,
                          track_type, rank, platform, platform_url, crawl_time, create_time, status))
        self.conn.commit()

    def delete_sku(self, _uuid):
        sql = "delete from scb_crawler_global_sku where scgs_uuid='%s'" % _uuid
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def delete_poa(self, _uuid):
        sql = "delete from scb_crawler_global_poa where scgs_uuid='%s'" % _uuid
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def select_sku_status9(self):
        sql = "select scgs_uuid, scgs_product_url from scb_crawler_global_sku where scgs_platform='dx' and scgs_status=9"
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def select_sku_by_url_create_time(self):
        sql = "select scgs_product_url from scb_crawler_global_sku where scgs_product_url like 'http://www.dx.com%' and scgs_create_time='2017-08-21 14:57:40'"
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def select_poa(self):
        sql = "select scgs_uuid,scgp_price, scgp_url from scb_crawler_global_poa where scgp_platform='dx' order by scgp_price desc limit 201"
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def select_poa_by_platform(self, domain):
        sql = 'select  scgp_id, scgs_uuid, scgs_product_id, scgp_product_id, scgp_url, scgp_price, scgp_original_price,' \
              'scgp_discount, scgp_attribute, scgp_review_count, scgp_grade_count, scgp_crawl_time, scgp_create_time,' \
              'scgp_status, scgp_platform from scb_crawler_global_poa where scgp_platform="%s" limit 1' % domain
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchone()

    def close(self):
        cursor = self.conn.cursor()
        cursor.close()
        self.conn.close()


if __name__ == '__main__':
    import time
    crawl_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    store = DxStore()
    store.insert_global_track('6b5d819cc19650198fc0487301602987', '123', 'http', 'abc', 11, 'ccc', 'http', '3', 'hot', 1,
                              'dx', 'dx.com', crawl_time, create_time, 0)




