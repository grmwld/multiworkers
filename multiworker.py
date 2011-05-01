#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import time
import itertools
import multiprocessing
import Queue
import argparse
import traceback
import numpy


class Worker(multiprocessing.Process):
    def __init__(self, work_queue, result_queue, verbose):
        super(Worker, self).__init__()
        self._work_queue = work_queue
        self._result_queue = result_queue
        self._kill_received = False
        self._verbose = verbose

    def _print_verbose(self, msg):
        if self._verbose:
            sys.stdout.write(msg)
            sys.stdout.flush()

    def run(self):
        while not self._kill_received:
            try:
                job = self._work_queue.get(True, 0.1)
            except Queue.Empty:
                break
            self.do(job)

    def do(self, job):
        self._result_queue.put(job)


class Controller:
    def __init__(self, input, num_cpu, verbose):
        self._input = input
        self._num_cpu = num_cpu
        self._verbose = verbose
        self._work_queue = multiprocessing.Queue()
        self._num_jobs = 0
        for param_set in self._input:
            param_set['id'] = self._num_jobs
            self._work_queue.put(param_set)
            self._num_jobs += 1
        self._result_queue = multiprocessing.Queue()
        self._results = []
        self._workers = []
        self._init_workers()

    def _init_workers(self):
        for i in range(self._num_cpu):
            worker = Worker(
                    self._work_queue,
                    self._result_queue,
                    self._verbose
            )
            self._workers.append(worker)

    def _finish(self):
        self._print_verbose('Finishing ...')
        self._results.sort(cmp=lambda x,y: x['id'] - y['id'])
        for result in self._results:
            print result
        self._print_verbose(' done !')

    def _print_verbose(self, msg):
        if self._verbose:
            sys.stdout.write(msg)
            sys.stdout.flush()

    def start(self):
        try:
            for worker in self._workers:
                worker.start()
            while len(self._results) < self._num_jobs:
                result = self._result_queue.get()
                self._results.append(result)
        except Exception as err:
            traceback.print_exc()
        finally:
            self._finish()


if __name__ == '__main__':
    c = Controller([{'name':'lskdfjs'}, {'name':'ksljdfs'}, {'name':'ksjdnfs'}, {'name':'ggtuna'}], 1, True)
    c.start()
