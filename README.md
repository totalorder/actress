actress
=======
```python
from scheduler import Scheduler

def consumer(s, pid, producer_pid):
    while 1:
        msg = yield

        if msg is s.sentinel:
            break
        print u"consumer received message: %s" % msg
        s.send(producer_pid, u"hello from %s!" % pid)

def producer(s, pid):
    child_pids = []
    for i in xrange(4):
        child_pids.append(s.spawn(consumer, pid))

    messages = 0
    for child_pid in child_pids:
        s.send(child_pid, u"hello %s?" % child_pid)
        messages += 1

    while messages:
        message = yield
        print u"producer received message: %s" % message
        messages -= 1
    for child_pid in child_pids:
        s.send(child_pid, s.sentinel)

s = Scheduler(pool_size=4)
producer_pid = s.spawn(producer)
s.run()
```
