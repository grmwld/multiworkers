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
    def __init__(self, work_queue, result_queue, verbose, global_params):
        super(Worker, self).__init__()
        self._global_params = global_params
        self._work_queue = work_queue
        self._result_queue = result_queue
        self._kill_received = False
        self._verbose = verbose

    def _print_verbose(self, msg):
        if self._verbose:
            sys.stdout.write(msg)
            sys.stdout.flush()

    def kill(self):
        self._kill_received = True

    def run(self):
        while not self._kill_received:
            try:
                job = self._work_queue.get(True, 0.1)
            except Queue.Empty:
                break
            self._result_queue.put(self.do(job))

    def do(self, job):
        result = job
        result.update(self._global_params)
        return result


class Controller:
    def __init__(self, jobs, global_params, num_cpu, verbose, worker_class=Worker, debug=False):
        self._jobs = jobs
        self._global_params = global_params
        self._num_cpu = num_cpu
        self._verbose = verbose
        self._worker_class = worker_class
        self._work_queue = multiprocessing.Queue()
        self._num_jobs = 0
        for job in self._jobs:
            job['id'] = self._num_jobs
            self._work_queue.put(job)
            self._num_jobs += 1
        self._result_queue = multiprocessing.Queue()
        self._results = []
        self._workers = []
        self._init_workers()
        self._debug = debug

    def _init_workers(self):
        for i in range(self._num_cpu):
            worker = self._worker_class(
                    self._work_queue,
                    self._result_queue,
                    self._verbose,
                    self._global_params
            )
            self._workers.append(worker)

    def _cleanup(self):
        for worker in self._workers:
            self._print_verbose('killing ' + str(worker))
            worker.kill()
            time.sleep(0.3)

    def _finish(self):
        self._print_verbose('Finishing ...')
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
                self._results.append(self._result_queue.get())
        except:
            if self._debug:
                print err
        finally:
            self._finish()

