from store import DxStore, DxRedis
from configuration_file import PoaUuid


def update_track():
    rds_12 = DxRedis(12)
    dx_store = DxStore()
    rows = dx_store.select_track_by_status(-1)
    for row in rows:
        poa_id = row['scgs_products_id']
        row_id = row['scgst_id']
        poa_id_key = PoaUuid + poa_id
        if rds_12.exists_key(poa_id_key):
            _uuid = rds_12.get_hash_field(poa_id_key, 'uuid')
            if isinstance(_uuid, bytes):
                _uuid = _uuid.decode('utf-8')
            dx_store.update_track_uuid_status(_uuid, 0, row_id)
    dx_store.close()


if __name__ == '__main__':
    pass
