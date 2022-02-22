from cassandra.cluster import Cluster
from collections import deque
from bw_utils import *
from datetime import datetime
from loguru import logger
import json, setproctitle, logging, os, multiprocessing, sys


__AWS_HOST__ = ['web.betterwhyiot.com']
__TARGET_HOST__ = ['localhost']
__KEYSPACE__ = 'betterwhy_bms'


# timestamp logics
__TIMESTAMP_LOGIC__ = {
    'bms_events_history': 1,
    'bms_stat_hour': 2,
    'bms_stat_day': 2,
    'bms_stat_month': 2,
    'bms_nominal_cr': 1,
    'bms_nominal_cr_stat': 1,
    'bms_v_piece': 3,
    'bms_volt_bias': 1,
    'bms_volt_bias_stat': 1,
    'bms_volt_bias_corr': 1,
    'bms_dcir': 1,
    'bms_dcir_stat': 1,
    'bms_dcir_corr': 1,
    'bms_v_cell_corr': 3,
    'bms_slope': 1,
    'bms_effective_cr': 1,
    'bms_alarms': 4,
}


def get_tables(host):
    print(f'try to get tables from {host}')
    cluster = Cluster(host)
    session = cluster.connect()

    ret_table = {}
    c_list = session.execute(f"SELECT table_name, column_name, clustering_order, kind, position, type FROM system_schema.columns WHERE keyspace_name='{__KEYSPACE__}'").all()
    cluster.shutdown()

    for col in c_list:
        if col.table_name not in ret_table:
            ret_table[col.table_name] = {}

        ret_table[col.table_name][col.column_name] = {
            'clustering_order': col.clustering_order,
            'kind': col.kind,
            'position': col.position,
            'type': col.type
        }

    print(f'getting tables finish')
    return ret_table


def get_tablecopy_info():
    AWS_TABLES = get_tables(__AWS_HOST__)
    TRG_TABLES = get_tables(__TARGET_HOST__)

    r_table = {}

    # print(f"{'*' * 20} exist tables {'*' * 20}")
    for table in TRG_TABLES:
        if table == 'bms_records':
            continue

        if table in AWS_TABLES:
            t_table = TRG_TABLES[table]
            a_table = AWS_TABLES[table]

            table_info = {}
            if table in __TIMESTAMP_LOGIC__:
                table_info['timestamp'] = __TIMESTAMP_LOGIC__[table]

            fld_alter = {}
            for fld in t_table:
                if fld in a_table:
                    if t_table[fld]['type'] != a_table[fld]['type']:
                        # print(f"[{table}] field type diff: {fld} - {a_table[fld]['type']} => {t_table[fld]['type']}")
                        fld_alter[fld] = t_table[fld]['type']

            for fld in t_table:
                if fld not in a_table:
                    # print(f'[{table}] field added: {fld}')
                    if fld != 'timestamp':
                        print(f'unexpected field added {fld}')

            fld_rem = []
            for fld in a_table:
                if fld not in t_table:
                    # print(f'[{table}] field removed: {fld}')
                    fld_rem.append(fld)

            if len(fld_alter) > 0:
                table_info['fld_alter'] = fld_alter

            if len(fld_rem) > 0:
                table_info['fld_ignore'] = fld_rem

            r_table[table] = table_info

    # print(f"{'*' * 20} added tables {'*' * 20}")
    # for table in TRG_TABLES:
    #     if table not in AWS_TABLES:
    #         print(f'[{table}] table added')

    # print(f"{'*' * 20} removed tables {'*' * 20}")
    # for table in AWS_TABLES:
    #     if table not in TRG_TABLES:
    #         print(f'[{table}] table removed')

    return r_table

print('-' * 80)

r_table = get_tablecopy_info()
print('-' * 80)
print(r_table)

print('-' * 80)

for t in r_table:
    if 'fld_alter' in r_table[t]:
        print(t, r_table[t]['fld_alter'])