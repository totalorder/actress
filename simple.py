import threading


class SimpleScheduler:
    def __init__(self):
        self.die = object()
        self.result = 0

    def send(self, target_pid, message):
        if target_pid == 0:
            self.result += message

class SimpleWorker(threading.Thread):
    def __init__(self, task, runs, loops, *args, **kwargs):
        self.task = task
        self.loops = loops
        self.runs = runs
        super(SimpleWorker, self).__init__(*args, **kwargs)

    def run(self):
        self.task.send(None)
        for i in xrange(self.runs):
            try:
                self.task.send(self.loops)
            except StopIteration:
                break
