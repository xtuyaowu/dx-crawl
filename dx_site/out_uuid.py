from store import DxStorePro, DxRedis

dx_rds = DxRedis(12)
# dx_store = DxStorePro()
# sql = "select scgs_uuid from scb_crawler_global_sku where scgs_platform='dx'"
# rows = dx_store.execute_sql(sql)
# for row in rows:
#     dx_rds.add_set('uuid', row['scgs_uuid'])


with open('uuid.txt', encoding='utf-8') as f:
    for line in f:
        line = line.strip()  # 去掉换行符
        dx_rds.add_set('uuid', line)

