from datetime import datetime
import time


def cmp_to_key(mycmp):
    class K:
        def __init__(self, obj, *args):
            self.obj = obj
        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0
        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0
        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0
        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0
        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0
        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0
    return K


def keytotimestamp(datekey, timekey, ms_dot):
    date = datetime(int(datekey / 10000), int((datekey / 100) % 100), int(datekey % 100), int(timekey / 10000), int((timekey / 100) % 100), int(timekey % 100))
    return int((time.mktime(date.timetuple()) * 1000) + (ms_dot * 1000))


def timestamptokey(tstamp):
    dt = datetime.fromtimestamp(int(tstamp / 1000))
    return int((dt.year * 10000) + (dt.month * 100) + (dt.day)), int((dt.hour * 10000) + (dt.minute * 100) + (dt.second))
    


