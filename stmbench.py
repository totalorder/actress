# encoding: utf-8
import os
import pstats
import time
import sys
import threading
import thread
import scheduler
import scheduler_nospinlock #as scheduler
import cProfile
import logging

#print sys.version
try:
    from __pypy__.thread import atomic
except ImportError:
    #print "Couldn't import __pypy__.thread.atomic, using thread.allocate_lock()"
    atomic = thread.allocate_lock()

class Worker(threading.Thread):
    def __init__(self, fun, *args, **kwargs):
        self.fun, self.args, self.kwargs = fun, args, kwargs
        super(Worker, self).__init__()

    def run(self):
        #with atomic:
        profiler = cProfile.Profile()
        profiler.runcall(self.fun, *self.args, **self.kwargs)
        profiler.print_stats('tottime')
        #stats = pstats.Stats(profiler)
        #stats.add(profiler.stats)
        #stats.sort_stats('tottime')
        #stats.print_stats()
        #profiler.stats
        #self.fun(*self.args, **self.kwargs)

class Runner:
    def __init__(self, fun, runs=1000):
        self.fun, self.runs = fun, runs
        try:
            from __pypy__.thread import getsegmentlimit
            self.num_threads = getsegmentlimit()
        except ImportError:
            #print "Couldn't call getsegmentlimit(), using num_threads = 4"
            self.num_threads = 4

    def run(self, num_threads=None, transaction_py=False, actress_messages=0, actress_consumers=0, scheduler_module=scheduler):
        if num_threads is None:
            num_threads = self.num_threads


        if num_threads and self.runs / num_threads < 1:
            raise Exception("runs / num_threads must be > 1!")
        if transaction_py:
            try:
                import transaction
            except ImportError:
                print "Couldn't import transaction, skipping!"
                return
            [transaction.add(self.fun, self.runs / num_threads * i, self.runs / num_threads) for i in xrange(num_threads)]
            start_t = time.time()
            transaction.run()
            print num_threads, time.time() - start_t, "(transaction)"
        elif actress_messages:
            if not actress_consumers:
                actress_consumers = num_threads
            if self.runs / actress_messages < 1:
                raise Exception("runs / actress must be > 1!")

            s = scheduler_module.Scheduler(num_threads)

            def producer(s, pid):
                child_pids = []
                for i in xrange(actress_consumers):
                    child_pids.append(s.spawn(scheduler_module.looper, self.fun, pid))

                total_messages = actress_messages
                messages = 0
                while messages < total_messages:
                    for i, child_pid in enumerate(child_pids):
                        s.send(child_pid, (messages * self.runs / total_messages, self.runs / total_messages))
                        messages += 1
                        if messages == total_messages:
                            break

                start_sending = time.time()
                resend_ratio = 0.8
                while messages:
                    sender_pid, data = yield
                    messages -= 1
                    if os.getenv('CONT', None) and messages <= total_messages * resend_ratio:
                        print "msg/sec: %s" % (total_messages * (1 - resend_ratio) / (time.time() - start_sending))
                        start_sending = time.time()
                        while messages < total_messages:
                            for i, child_pid in enumerate(child_pids):
                                s.send(child_pid, (messages * self.runs / total_messages, self.runs / total_messages))
                                messages += 1
                                if messages == total_messages:
                                    break
                    #print "finito", sender_pid, time.time() - start_t
                for child_pid in child_pids:
                    s.send(child_pid, s.sentinel)
            s.spawn(producer)
            start_t = time.time()
            if os.getenv('PROFILE', None):
                profile = cProfile.Profile()
                profile.runcall(s.run)
                profile.print_stats('tottime')
                scheduler_module.tprof.dumpProfile()
            else:
                s.run()
            print num_threads, time.time() - start_t, "(actress %s %s %s)" % (actress_messages, actress_consumers, scheduler_module.__name__)

        else:
            if num_threads:
                threads = [Worker(self.fun, i * self.runs / num_threads, self.runs / num_threads) for i in range(num_threads)]
                start_t = time.time()
                [t.start() for t in threads]
                [t.join() for t in threads]
                print num_threads, time.time() - start_t
            else:
                start_t = time.time()
                self.fun(0, self.runs)
                print num_threads, time.time() - start_t


def work(start, length):
    v = 0
    for _ in xrange(length):
        v = (v + 1) % 100


def mandel(start, length):
    def _mandel(c):
        z = 0
        for _ in xrange(0, 20):
            z = z ** 2 + c
            if abs(z) > 2:
                break
        if abs(z) >= 2:
            return 0
        else:
            return 1

    columns = 10000
    for x in xrange(length):
        row_n = start + x
        real = row_n / 200.0 - 1.5
        row = []
        for y in range(0, columns):
            img = y / 200.0 - 1.5
            c = complex(real, img)
            row.append(_mandel(c))


#logging.getLogger().setLevel("INFO")
#logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))

r = Runner(mandel, 1000)
#r.run(num_threads=1)
if os.getenv('NOSPIN'):
    r.run(actress_messages=200, scheduler_module=scheduler_nospinlock, actress_consumers=50)
else:
    r.run(actress_messages=200, actress_consumers=50)

#r.run(actress=100)
#r.run(actress=100, scheduler_module=scheduler_nospinlock)
#r.run(actress=100)
#r.run(actress=1000, scheduler_module=scheduler_nospinlock)
#r.run(actress=4)
#r.run(actress=4, scheduler_module=scheduler_nospinlock)
#r.run(actress=500)
#r.run(actress=500, scheduler_module=scheduler_nospinlock)
#r.run(actress=500, actress_consumers=100)
#r.run(actress=500, scheduler_module=scheduler_nospinlock, actress_consumers=100)
#r.run(actress=500, actress_consumers=500)
#r.run(actress=500, scheduler_module=scheduler_nospinlock, actress_consumers=500)

#r.run(actress=500)
#r.run(actress=500, scheduler_module=scheduler_nospinlock)
#r.run(actress=500)
#r.run(actress=500, scheduler_module=scheduler_nospinlock)

#r.run(num_threads=1)
#r.run(num_threads=0)
#r.run(num_threads=4, transaction_py=True)
#r.run(num_threads=1, actress=True)

#r.run(actress=100)
#r.run(actress=100, scheduler_module=scheduler_nospinlock)
#r.run(actress=100)
#r.run(actress=100, scheduler_module=scheduler_nospinlock)
#r.run(actress=100)
#r.run(actress=100, scheduler_module=scheduler_nospinlock)


#r.run()
#r.run(actress=100)
#r.run(actress=4)
#r.run()
#r.run(actress=100)
#r.run(actress=4)
#r.run()
#r.run(actress=100)
#r.run(actress=4)


#r = Runner(work, 50000)
#r.run(num_threads=4, actress=100)


#r.run(work, 500000000)
#r.run(work, 500000000, num_threads=1)
#r.run(work, 500000000, num_threads=0)
#r.run(work, 500000000, num_threads=4, transaction_py=True)





