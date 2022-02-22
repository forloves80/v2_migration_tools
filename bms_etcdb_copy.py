from cassandra.cluster import Cluster
from collections import deque
from bw_utils import *
from datetime import datetime
from loguru import logger
import json, setproctitle, logging, os, multiprocessing, sys, time


__APP_NAME__ = 'bw_etc_copy'

__AWS_HOST__ = ['web.betterwhyiot.com']
__TARGET_HOST__ = ['localhost']
__KEYSPACE__ = 'betterwhy_bms'
__NOTI_COUNT__ = 100000
__SUB_NOTI_COUNT__ = 1000

logging.getLogger().setLevel(logging.DEBUG)
logger.add(os.path.join(f'log/{__APP_NAME__}', '{time:YYYYMMDD-HHmmss}.log'))

def log_d(msg):
    logger.opt(depth=1).debug(msg)

def log_e(msg):
    logger.opt(depth=1).error(msg)


setproctitle.setproctitle(__APP_NAME__)


def f_cmp_index(x, y):
    if x['timeindex'] < y['timeindex']:
        if x['timeindex'] <= 1 and y['timeindex'] >= 65534:
            return 1
        return -1
    elif x['timeindex'] > y['timeindex']:
        if x['timeindex'] >= 65534 and y['timeindex'] <= 1:
            return -1
        return 1

    return 0

def mstimestamp(dt):
    return int((time.mktime(dt.timetuple()) * 1000) + (dt.microsecond / 1000))

def create_timestamp(data, ts_logic):
    dt = None

    try:
        if ts_logic == 1:
            dt = datetime(int(data.datekey / 10000), int((data.datekey / 100) % 100), int(data.datekey % 100), int(data.timekey / 10000), int((data.timekey / 100) % 100), int(data.timekey % 100))
        elif ts_logic == 2:
            dt = datetime(int(data.datekey / 1000000), int((data.datekey / 10000) % 100), int((data.datekey / 100) % 100), int(data.datekey % 100))
        elif ts_logic == 3:
            dt = datetime(int(data.datekey / 10000), int((data.datekey / 100) % 100), int(data.datekey % 100))
        elif ts_logic == 4:
            dt = datetime(int(data.datetimekey / 10000000000), int((data.datetimekey / 100000000) % 100), int((data.datetimekey / 1000000) % 100), int((data.datetimekey / 10000) % 100), int((data.datetimekey / 100) % 100), int(data.datetimekey % 100))
    except Exception as e:
        print('create_timestamp exception', data)
        raise e


    if dt:
        return mstimestamp(dt)

    return None



class ClientDBSaver(multiprocessing.Process):

    def __init__(self, table_name, table_info, clientid):
        multiprocessing.Process.__init__(self, daemon=True)
        self.clientid = clientid
        self.name = f'CP_{table_name}_{clientid}'
        self.table_name = table_name
        self.table_info = table_info
        self.dataque = deque()
        self.save_count = 0

    def isIn(self, raw):
        return (raw.clientid == self.clientid)

    def add(self, raw):
        self.dataque.append(raw)

    def run(self):
        setproctitle.setproctitle(self.name)
        # log_d(f'[{self.name}] start saving process')
        try:
            cluster = Cluster(__TARGET_HOST__)
            self.session = cluster.connect(__KEYSPACE__)

            ts_logic = 0
            if self.table_name in __TIMESTAMP_LOGIC__:
                ts_logic = __TIMESTAMP_LOGIC__[self.table_name]

            old_timestamp = None
            if ts_logic > 0 and ts_logic < 4:
                query = f"SELECT * FROM {self.table_name} WHERE clientid='{self.clientid}' ORDER BY timestamp ASC limit 1"
                saved = self.session.execute(query)
                if saved and len(saved.current_rows) > 0:
                    saved_data = saved.one()
                    old_timestamp = mstimestamp(saved_data.timestamp)

            try:
                while True:
                    data = self.dataque.popleft()
                    data_timestamp = None
                    if ts_logic > 0:
                        data_timestamp = create_timestamp(data, ts_logic)

                    if old_timestamp and data_timestamp >= old_timestamp:
                        continue

                    rdict = data._asdict()
                    if data_timestamp:
                        rdict['timestamp'] = data_timestamp

                    if 'fld_alter' in self.table_info:
                        for fld in self.table_info['fld_alter']:
                            if rdict[fld] != None:
                                if self.table_info['fld_alter'][fld] == 'int':
                                    rdict[fld] = int(rdict[fld])
                                elif self.table_info['fld_alter'][fld] == 'float':
                                    rdict[fld] = float(rdict[fld])

                    if 'fld_ignore' in self.table_info:
                        for fld in self.table_info['fld_ignore']:
                            del rdict[fld]

                    query_yes = True
                    if self.table_name in __IGNORE__:
                        for ig in __IGNORE__[self.table_name]:
                            if rdict[ig['field']] == ig['value']:
                                query_yes = False
                                break


                    if query_yes:
                        self.session.execute(f"INSERT INTO {self.table_name} JSON '{json.dumps(rdict)}'")
                        self.save_count += 1

            except IndexError:
                pass

            cluster.shutdown()
        except KeyboardInterrupt:
            pass

        # log_d(f'[{self.name}] saving process finish: {self.save_count:,}')


def manage_process(proc_list):
    proc_cnt = len(proc_list)
    alive_cnt = 0
    rem_list = []
    for proc in proc_list:
        if proc.is_alive():
            alive_cnt += 1
        else:
            rem_list.append(proc)

    for proc in rem_list:
        proc_list.remove(proc)
        del proc

    log_d(f'[MAIN] process info alive: {alive_cnt}, total: {proc_cnt}')



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
#     'bms_alarms': 4,
}

__IGNORE__ = {
    'bms_events_history': [
        {'field': 'type', 'value': 5},
    ]
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


__SKIP_LIST__ = ['bms_records', 'bms_alarms', 'weather']


def get_tablecopy_info():
    AWS_TABLES = get_tables(__AWS_HOST__)
    TRG_TABLES = get_tables(__TARGET_HOST__)

    r_table = {}

    # print(f"{'*' * 20} exist tables {'*' * 20}")
    for table in TRG_TABLES:
        if table in __SKIP_LIST__:
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


def main():
    proc_list = []
    cur_proc = None

    table_info = get_tablecopy_info()

    try:
        log_d('[MAIN] try to connect AWS Cassandra')
        cluster = Cluster(__AWS_HOST__)
        session = cluster.connect(__KEYSPACE__)

        cnt = 0
        noti = __NOTI_COUNT__

        for table in table_info:
            log_d(f'[MAIN] try to query {table}')
            ret = session.execute(f'SELECT * FROM {table}')

            while True:
                for r in ret.current_rows:
                    if not cur_proc or not cur_proc.isIn(r):
                        if cur_proc:
                            cur_proc.start()
                            proc_list.append(cur_proc)
                            # manage_process(proc_list)

                        cur_proc = ClientDBSaver(table, table_info[table], r.clientid)

                    cur_proc.add(r)

                if not ret.has_more_pages:
                    break

                ret.fetch_next_page()

            if cur_proc:
                cur_proc.start()
                proc_list.append(cur_proc)
                cur_proc = None

            manage_process(proc_list)

        log_d('[MAIN] wait process finish')
        for proc in proc_list:
            proc.join()

        cluster.shutdown()
        log_d('[MAIN] shutdown')

    except KeyboardInterrupt:
        log_d('[MAIN] user stop')


if __name__ == '__main__':
    main()

