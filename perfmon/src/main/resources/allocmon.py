import sys
import threading
import config
import codecs
import signal
from time import sleep

sys.path.append(config.hdbclient_path)
from hdbcli import dbapi


class AllocatorMonitor:

    locker = threading.Lock()
    conn = None
    curr = None
    monitor = None
    stopped = False
    data = list()

    def __init__(self):
        self.conn = dbapi.connect(config.db_host, config.db_port, config.db_user, config.db_pass)
        self.curr = self.conn.cursor()

    def __enter__(self):
        print "Starting monitor..."
        self.monitor = threading.Thread(target=self.perform_monitoring, name="DbMonitor")
        self.data = list()
        self.monitor.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print "Stopping monitor..."
        self.stop()
        if self.curr:
            self.curr.close()
        if self.conn:
            self.conn.close()

    def stop(self):
        with self.locker:
            self.stopped = True
        self.monitor.join()

    def perform_monitoring(self):
        print "Monitor started"
        curr = self.curr
        while True:
            curr.execute(config.mem_query)
            ret = curr.fetchall()
            self.data.append(ret)
            sleep(config.mem_query_interval)
            with self.locker:
                if self.stopped:
                    break
        self.save_data()
        print "Monitor stopped"

    def save_data(self):
        print "Saving statistics..."
        separator = '\t'
        lines = list()
        depth0 = list()
        depth1 = list()
        depth2 = list()
        for line in self.data:
            depth0.extend(filter(lambda x: x[2] == 0, line))
            depth1.extend(filter(lambda x: x[2] == 1, line))
            depth2.extend(filter(lambda x: x[2] == 2, line))
        if len(depth0) > 0:
            s = str(depth0[0][0]) + separator + str(depth0[0][1])
            for d0 in depth0:
                s += separator + str(d0[3])
            lines.append(s)
        categories_d1 = set([x[1] for x in depth1])
        for category in categories_d1:
            stats = filter(lambda x: x[1] == category, depth1)
            if len(stats) > 0:
                s = str(stats[0][0]) + separator + str(stats[0][1])
                for st in stats:
                    s += separator + str(st[3])
                lines.append(s)
        categories_d2 = set(x[1] for x in depth2)
        for category in categories_d2:
            stats = filter(lambda x: x[1] == category, depth2)
            if len(stats) > 0:
                s = str(stats[0][0]) + separator + str(stats[0][1])
                for st in stats:
                    s += separator + str(st[3])
                lines.append(s)
        with codecs.open(config.output_file, "w+") as f:
            for line in lines:
                f.write(line + '\n')
        print "Statistics saved"


if __name__ == '__main__':
    with AllocatorMonitor() as alloc:
        def signal_handler(signal, frame):
            print 'Stopping...'
            sys.exit(0)
        signal.signal(signal.SIGINT, signal_handler)
        print "Press CTRL+C to stop monitor..."
        if sys.platform.startswith("win"):
            while True:
                sleep(1024)
        else:
            signal.pause()