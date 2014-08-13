# encoding: utf-8
import cProfile
import pstats
import threading
import types


class ThreadProfiler(object):
    def __init__(self):
        self.profiles = []

    def getProfiledRun(self, cls):
        this = self
        original_run = cls.run

        def _run(self):
            profiler = cProfile.Profile()
            profiler.runcall(original_run, self)
            this.profiles.append(profiler)
        return _run
        #return types.MethodType(run, cls)

    def dumpProfile(self):
        if self.profiles:
            stats = pstats.Stats(self.profiles[0])
            for profile in self.profiles[1:]:
                stats.add(profile)
            stats.sort_stats('tottime')
            stats.print_stats()
        self.profiles = []


class RateProfiler(threading.Thread):
    def __init__(self, scheduler):
        self.scheduler = scheduler


