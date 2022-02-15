from cassandra.cluster import Cluster
from collections import deque
from bw_utils import *
from datetime import datetime
from loguru import logger
import json, setproctitle, logging, os, multiprocessing


__APP_NAME__ = 'bw_rec_copy'

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


class ClientDBSaver(multiprocessing.Process):

    def __init__(self, clientid, datekey):
        multiprocessing.Process.__init__(self, daemon=True)
        self.clientid = clientid
        self.datekey = datekey
        self.name = f'CP_{clientid}_{datekey}'
        self.dataque = deque()
        self.data_station = []
        self.save_count = 0

    def isIn(self, raw):
        return (raw.clientid == self.clientid) and (raw.datekey == self.datekey)

    def add(self, raw):
        self.dataque.append(raw)

    def run(self):
        setproctitle.setproctitle(self.name)
        log_d(f'[{self.name}] start saving process')
        try:
            cluster = Cluster(__TARGET_HOST__)
            self.session = cluster.connect(__KEYSPACE__)

            try:
                while True:
                    data = self.dataque.popleft()
                    self._addStation(data)

            except IndexError:
                pass

            self._saveToDB()

            cluster.shutdown()
        except KeyboardInterrupt:
            pass

        log_d(f'[{self.name}] saving process finish: {self.save_count:,}')

    def _addStation(self, data):
        rdict = data._asdict()
        rdict['sac'] = int(rdict['sac'])
        rdict['saac'] = int(rdict['saac'])
        if len(self.data_station) > 0 and self.data_station[0]['timekey'] != rdict['timekey']:
            self._saveToDB()
        self.data_station.append(rdict)

    def _saveToDB(self):
        r_cnt = len(self.data_station)
        if r_cnt <= 0:
            log_e(f'[{self.clientid}:{self.datekey}] no data to save')
            return

        self.data_station.sort(key=cmp_to_key(f_cmp_index))

        off = 0
        for data in self.data_station:
            data['timestamp'] = keytotimestamp(self.datekey, data['timekey'], off / r_cnt)
            off += 1
            self.session.execute(f"INSERT INTO bms_records JSON '{json.dumps(data)}'")
            self.save_count += 1
        self.data_station.clear()


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

def main():

    proc_list = []
    cur_proc = None

    try:
        log_d('[MAIN] try to connect AWS Cassandra')
        cluster = Cluster(__AWS_HOST__)
        session = cluster.connect(__KEYSPACE__)

        log_d('[MAIN] try to query')
        ret = session.execute('SELECT * FROM bms_records')

        cnt = 0
        noti = __NOTI_COUNT__
        while True:
            for r in ret.current_rows:
                if not cur_proc or not cur_proc.isIn(r):
                    if cur_proc:
                        cur_proc.start()
                        proc_list.append(cur_proc)
                        manage_process(proc_list)

                    cur_proc = ClientDBSaver(r.clientid, r.datekey)

                cur_proc.add(r)

            if not ret.has_more_pages:
                break

            ret.fetch_next_page()

        if cur_proc:
            cur_proc.start()
            proc_list.append(cur_proc)

        log_d('[MAIN] wait process finish')
        for proc in proc_list:
            proc.join()

        cluster.shutdown()
        log_d('[MAIN] shutdown')

    except KeyboardInterrupt:
        log_d('[MAIN] user stop')


if __name__ == '__main__':
    main()

