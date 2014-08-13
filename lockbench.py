# encoding: utf-8
import time
import threading

global_state = []
global_lock = threading.Lock()

class Worker(threading.Thread):
    def __init__(self, runs):
        self.runs = runs
        super(Worker, self).__init__()

    def run(self):
        #with global_lock:
            time.sleep(5-self.runs)
            global_state.append(self.runs)


for _ in xrange(1):
    n_workers = 4
    workers = [Worker(i) for i in xrange(n_workers)]
    #start_time = time.time()
    [w.start() for w in workers]
    [w.join() for w in workers]
    #print time.time() - start_time
    print global_state