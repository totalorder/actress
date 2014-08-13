# encoding: utf-8
from scheduler import Scheduler
from Queue import Queue
import time


def mandel(c):
    z = 0
    for _ in xrange(0, 20):
        z = z ** 2 + c
        if abs(z) > 2:
            break
    if abs(z) >= 2:
        return 0
    else:
        return 1


def consumer(s, pid, producer_pid):
    while 1:
        msg = yield
        if msg is s.die:
            break

        columns, row_n, real = msg
        row = []
        for y in range(0, columns):
            img = y / 200.0 - 1.5
            c = complex(real, img)
            #print x, y
            row.append(mandel(c))
        s.send(producer_pid, (row_n, row))


def producer(s, pid, result_queue):
    child_pids = []
    for _ in xrange(4):
        child_pids.append(s.spawn(consumer, pid))

    rows, columns = 10000, 10000
    messages = 0
    child_idx = 0
    result = []
    for x in xrange(rows):
        result.append([])
        real = x / 200.0 - 1.5
        s.send(child_pids[child_idx], (columns, x, real))
        messages += 1
        child_idx = (child_idx + 1) % len(child_pids)

    while messages:
        message = yield
        row_n, row = message
        result[row_n] = row
        messages -= 1
    for child_pid in child_pids:
        s.send(child_pid, s.die)
    result_queue.put(result)

q = Queue()
s = Scheduler(pool_size=8)
start_time = time.time()
producer_pid = s.spawn(producer, q)
s.run()
print time.time() - start_time

try:
    import matplotlib.pyplot as plt
    import numpy as np

    plt.imshow(np.array(q.get()))
    plt.show()
except ImportError:
    print "error importing matplotlib"