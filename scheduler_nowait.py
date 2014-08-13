# encoding: utf-8
import threading
import time
from Queue import Queue, Empty
import sys

class Worker(threading.Thread):
    def __init__(self, die, dead_task, queue, messages, debug_log, *args, **kwargs):
        self.die = die
        self.dead_task = dead_task
        self.queue = queue
        self.messages = messages
        self.debug_log = debug_log
        self.runs = 0
        self.useful_runs = 0
        super(Worker, self).__init__(*args, **kwargs)

    def run(self):
        try:
            while 1:
                pid = self.queue.get_nowait()
                #if pid is self.die:
                #    self.debug_log.append("runs %s useful %s" % (self.runs, self.useful_runs))
                #    return
                self.runs += 1
                try:
                    task, message = self.messages[pid].get_nowait()
                    self.useful_runs += 1
                    try:
                        task.send(message)
                        self.queue.put(pid)
                    except StopIteration:
                        pass
                        #self.dead_task(pid)
                except Empty:
                    self.queue.put(pid)
        except Empty:
            pass
        self.debug_log.append("runs %s useful %s" % (self.runs, self.useful_runs))


class Scheduler:
    def __init__(self, poolsize=4):
        self.poolsize = poolsize
        self.pool = []
        self.tasks = {}
        self.messages = {}
        self.queue = Queue()
        self.sequence_id = 0
        self.die = object()
        self.debug_log = []  # Not thread safe. Let's go STM! =)

    def run(self):
        for i in range(0, self.poolsize):
            w = Worker(self.die, self.dead_task, self.queue, self.messages, self.debug_log)
            self.pool.append(w)
        [w.start() for w in self.pool]
        [w.join() for w in self.pool]
        print '\n'.join(self.debug_log)

    def dead_task(self, task_id):
        # This is the race track! (for thread races)
        del self.tasks[task_id]
        if not self.tasks:
            [self.queue.put(self.die) for _ in self.pool]

    def spawn(self, generator, *args, **kwargs):
        self.sequence_id += 1
        task = generator(self, self.sequence_id, *args, **kwargs)
        self.tasks[self.sequence_id] = task
        self.messages[self.sequence_id] = Queue()
        self.messages[self.sequence_id].put((task, None))
        self.queue.put(self.sequence_id)
        return self.sequence_id

    def send(self, target_pid, message):
        self.messages[target_pid].put((self.tasks[target_pid], message))


def looper(s, pid, fun, receiver_pid):
    while 1:
        msg = yield

        if msg is s.die:
            break
        s.send(receiver_pid, (pid, fun(*msg)))
