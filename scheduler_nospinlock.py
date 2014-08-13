# encoding: utf-8
import cProfile
import os
import threading
import thread
import logging
import sys
import time
import threadprof

try:
    from __pypy__.thread import atomic
except ImportError:
    #print "Couldn't import __pypy__.thread.atomic, using thread.allocate_lock()"
    atomic = thread.allocate_lock()

class _logging:
    def __init__(self):
        self.log = []

    def info(self, *args):
        self.log.append(' '.join(args))
        #print ' '.join(args)

    def dump(self):
        #with atomic:
            if self.log:
                print '\n'.join(self.log)
                self.log = []

logging = _logging()
tprof = threadprof.ThreadProfiler()

class Worker(threading.Thread):
    def __init__(self, worker_id, sentinel, dead_task_callback, ownership_lock_queue, message_queues, task_locks, *args, **kwargs):
        self.worker_id = worker_id
        self.sentinel = sentinel
        self.dead_task_callback = dead_task_callback
        self.ownership_lock_queue = ownership_lock_queue
        self.message_queues = message_queues
        self.task_locks = task_locks
        self.stat_runs = 0
        self.stat_useful_runs = 0
        super(Worker, self).__init__(*args, **kwargs)

    def run(self):
        try:
            while 1:
                #logging.dump()
                try:
                    pid = self.ownership_lock_queue.pop(0)
                    #logging.info("t%s-p%s-%s" % (str(self.ident)[-1], pid, '-'.join(map(str, self.ownership_lock_queue))))
                    if pid is self.sentinel:
                        #logging.info("worker %s: runs %s useful %s" % (self.ident, self.stat_runs, self.stat_useful_runs))
                        return
                    self.stat_runs += 1
                    try:
                        task, message = self.message_queues[pid].fetch_message()
                        self.stat_useful_runs += 1
                        try:
                            task.send(message)
                            self.message_queues[pid].message_processed()

                        except StopIteration:
                            self.dead_task_callback(pid)
                    except IndexError:
                        pass
                except IndexError:
                    pass
        except Exception as e:
            with atomic:
                print "error pid %s: %s" % (pid, str(e))
                self.dead_task_callback(pid, True)

if os.getenv('PROFILE', None):
    Worker.run = tprof.getProfiledRun(Worker)

class loglist(list):
    def append(self, obj):
        with atomic:
            print '-'.join(map(str, self)), "a:", obj
            if type(obj) is int and obj in self:
                raise Exception("Duplicate pid (%s)!" % obj)
        super(loglist, self).append(obj)

    def pop(self, index=None):
        obj = super(loglist, self).pop(index)
        with atomic:
            print '-'.join(map(str, self)), "p:", obj
        return obj

class LogLock():
    def __init__(self, task_id):
        self.task_id = task_id
        self.lock = thread.allocate_lock()
        #super(LogLock, self).__new__()

    def __enter__(self):
        self.lock.acquire()
        with atomic:
            print "aq!", self.task_id

    def __exit__(self, *args, **kwargs):
        with atomic:
            print "re!", self.task_id
        self.lock.release()

l = LogLock("asd")

class mq(list):
    def __init__(self, ownership_queue, task_id, task, *args, **kwargs):
        self.ownership_queue = ownership_queue
        self.task_id = task_id
        self.task = task
        self.lock = thread.allocate_lock()  # LogLock(self.task_id)
        self.has_token = True
        super(mq, self).__init__(*args, **kwargs)

    def new_message(self, message):
        q = False
        with self.lock:
            if not self and self.has_token:
                #with atomic:
                #    print "ma->",
                q = True
                self.has_token = False
            super(mq, self).append((self.task, message))
        if q:
            self.ownership_queue.append(self.task_id)

    def fetch_message(self):
        with self.lock:
            return super(mq, self).pop(0)
            #with atomic:
            #    print "mp->", v[1]

    def message_processed(self):
        q = False
        with self.lock:
            if not self.has_token and self:
                #with atomic:
                #    print "mr->", self.task_id
                q = True

            else:
                self.has_token = True
        if q:
            self.ownership_queue.append(self.task_id)

    def append(self, element):
        raise NotImplementedError("Use new_message")

    def pop(self, index=None):
        raise NotImplementedError("Use fetch_message")

class Scheduler:
    def __init__(self, pool_size=4):
        self.pool_size = pool_size
        self.pool = []
        self.tasks = {}
        self.message_queues = {}
        self.task_locks = {}
        self.ownership_lock_queue = [] #loglist()
        self.sequence_id = 0
        class _Sentinel:
            def __repr__(self):
                return "sentinel"
        self.sentinel = _Sentinel()

    def run(self):
        for i in xrange(self.pool_size):
            w = Worker(i, self.sentinel, self.dead_task, self.ownership_lock_queue, self.message_queues, self.task_locks)
            self.pool.append(w)
        [w.start() for w in self.pool]
        [w.join() for w in self.pool]
        logging.dump()
        #tprof.dumpProfile()

    def dead_task(self, task_id, worker_dead=False):
        del self.tasks[task_id]
        if not self.tasks or worker_dead:
            self._kill_workers()

    def _kill_workers(self):
        [self.ownership_lock_queue.append(self.sentinel) for _ in self.pool]

    def spawn(self, generator, *args, **kwargs):
        with atomic:
            self.sequence_id += 1
        task = generator(self, self.sequence_id, *args, **kwargs)
        self.tasks[self.sequence_id] = task
        self.message_queues[self.sequence_id] = mq(self.ownership_lock_queue, self.sequence_id, task)
        self.message_queues[self.sequence_id].new_message(None)
        return self.sequence_id

    def send(self, target_pid, message):
        self.message_queues[target_pid].new_message(message)

def looper(s, pid, fun, receiver_pid):
    while 1:
        msg = yield

        if msg is s.sentinel:
            break
        s.send(receiver_pid, (pid, fun(*msg)))
