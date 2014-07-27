# encoding: utf-8
import threading
import logging


class Worker(threading.Thread):
    def __init__(self, sentinel, dead_task_callback, ownership_lock_queue, message_queues, *args, **kwargs):
        self.sentinel = sentinel
        self.dead_task_callback = dead_task_callback
        self.ownership_lock_queue = ownership_lock_queue
        self.message_queues = message_queues
        self.stat_runs = 0
        self.stat_useful_runs = 0
        super(Worker, self).__init__(*args, **kwargs)

    def run(self):
        while 1:
            try:
                pid = self.ownership_lock_queue.pop(0)
                if pid is self.sentinel:
                    logging.info("worker %s: runs %s useful %s" % (self.ident, self.stat_runs, self.stat_useful_runs))
                    return
                self.stat_runs += 1
                try:
                    task, message = self.message_queues[pid].pop(0)
                    self.stat_useful_runs += 1
                    try:
                        task.send(message)
                        self.ownership_lock_queue.append(pid)
                    except StopIteration:
                        self.dead_task_callback(pid)
                except IndexError:
                    self.ownership_lock_queue.append(pid)
            except IndexError:
                pass


class Scheduler:
    def __init__(self, pool_size=4):
        self.pool_size = pool_size
        self.pool = []
        self.tasks = {}
        self.message_queues = {}
        self.ownership_lock_queue = []
        self.sequence_id = 0
        self.sentinel = object()

    def run(self):
        for i in xrange(self.pool_size):
            w = Worker(self.sentinel, self.dead_task, self.ownership_lock_queue, self.message_queues)
            self.pool.append(w)
        [w.start() for w in self.pool]
        [w.join() for w in self.pool]

    def dead_task(self, task_id):
        del self.tasks[task_id]
        if not self.tasks:
            [self.ownership_lock_queue.append(self.sentinel) for _ in self.pool]

    def spawn(self, generator, *args, **kwargs):
        self.sequence_id += 1
        task = generator(self, self.sequence_id, *args, **kwargs)
        self.tasks[self.sequence_id] = task
        self.message_queues[self.sequence_id] = []
        self.message_queues[self.sequence_id].append((task, None))
        self.ownership_lock_queue.append(self.sequence_id)
        return self.sequence_id

    def send(self, target_pid, message):
        self.message_queues[target_pid].append((self.tasks[target_pid], message))


def looper(s, pid, fun, receiver_pid):
    while 1:
        msg = yield

        if msg is s.sentinel:
            break
        s.send(receiver_pid, (pid, fun(*msg)))
