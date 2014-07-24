# encoding: utf-8
import threading
import time
from Queue import Queue, Empty
import sys

class Worker(threading.Thread):
    def __init__(self, queue, messages, *args, **kwargs):
        self.queue = queue
        self.messages = messages
        super(Worker, self).__init__(*args, **kwargs)

    def run(self):
        try:
            while 1:
                pid = self.queue.get_nowait()
                try:
                    task, message = self.messages[pid].get_nowait()
                    try:
                        task.send(message)
                        self.queue.put(pid)
                    except StopIteration:
                        pass
                except Empty:
                    self.queue.put(pid)
        except Empty:
            pass


class Scheduler:
    def __init__(self, poolsize = 4):
        self.poolsize = poolsize
        self.pool = []
        self.tasks = {}
        self.messages = {}
        self.queue = Queue()
        self.sequence_id = 0
        self.die = object()

    def run(self):
        for i in range(0, self.poolsize):
            w = Worker(self.queue, self.messages)
            self.pool.append(w)
        [w.start() for w in self.pool]
        [w.join() for w in self.pool]

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
