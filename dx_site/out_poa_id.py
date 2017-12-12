from store import DxStorePro, DxRedis

dx_store = DxStorePro()
dx_rds = DxRedis(12)

while True:
    item = dx_rds.pop_member('uuid')
    if not item:
        break
    if isinstance(item, bytes):
        item = item.decode('utf-8')
    sql = "select scgs_uuid, scgp_product_id from scb_crawler_global_poa where scgs_uuid=%s"
    rows = dx_store.execute_sql(sql, item)
    for row in rows:
        key = '{}{}'.format('dx:site:poa_id_uuid:', row['scgp_product_id'])
        dx_rds.set_hash(key, {'uuid': row['scgs_uuid']})
        #dx_rds.add_set('uuid', row['scgs_uuid'])
