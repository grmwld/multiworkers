#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import sys
import time
import datetime
import threading
import multiprocessing
import Queue
import traceback
from dateutil.relativedelta import relativedelta
from progressbar.progressbar import ProgressBar


class Worker(multiprocessing.Process):
    def __init__(self, work_queue, result_queue, current_queue, global_params):
        super(Worker, self).__init__()
        self._work_queue = work_queue
        self._result_queue = result_queue
        self._current_queue = current_queue
        self._global_params = global_params
        self._kill_received = False

    def kill(self):
        self._kill_received = True
        self.terminate()

    def update_progress(self, interval=1):
        tick = datetime.datetime.fromtimestamp(time.time())
        rd = relativedelta(tick, self.iTime)
        days = '%dd ' % (rd.days) if rd.days else ''
        hours = '%dh ' % (rd.hours) if rd.hours else ''
        minutes = '%dm ' % (rd.minutes) if rd.minutes else ''
        seconds = '%ds' % (rd.seconds)
        elapsed = ''.join([days, hours, minutes, seconds])
        self._current_queue.put({
            'job': self.job,
            'worker': self.name,
            'time': elapsed
        })
        self._looper = threading.Timer(
            interval=interval,
            function=self.update_progress,
            kwargs={'interval': interval}
        ).start()

    def run(self):
        self.iTime = datetime.datetime.fromtimestamp(time.time())
        self.job = None
        self.update_progress()
        while not self._kill_received:
            try:
                self.job = self._work_queue.get(True)
                self.iTime = datetime.datetime.fromtimestamp(time.time())
                if self.job is None:
                    raise Queue.Empty
                result = self.do(self.job)
                self._result_queue.put(result)
            except (Queue.Empty, KeyboardInterrupt):
                break
        self._current_queue.put({
            'job': self.job,
            'worker': self.name,
            'time': None
        })
        return

    def do(self, job):
        raise NotImplementedError


class Controller:
    def __init__(self, jobs, global_params, num_cpu=1, quiet=False,
                 worker_class=Worker, debug=False):
        self._jobs = jobs
        self._global_params = global_params
        self._num_cpu = num_cpu
        self._quiet = quiet
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
        self._ongoing_work = {}
        self._current_queue = multiprocessing.Queue()
        self._current = []
        self._init_workers()
        self._done_workers = 0
        self._debug = debug
        self._progress = ProgressBar('green', width=80, block='█', empty='█')
        self._progress_time = ''
        self._progress_counts = ''
        self._progress_workers = ''
        self._progress_message = ''
        self.abs_iTime = time.time()
        self.iTime = datetime.datetime.fromtimestamp(self.abs_iTime)

    def _init_workers(self):
        for i in range(self._num_cpu):
            worker = self._worker_class(
                self._work_queue,
                self._result_queue,
                self._current_queue,
                self._global_params,
            )
            self._workers.append(worker)
            self._work_queue.put(None)
            self._ongoing_work[worker.name] = {}

    def _update_progress(self, one_time=True, daemon=False):
        if one_time:
            percent = int((len(self._results) / float(self._num_jobs)) * 100)
            self._progress.render(percent, self.update_progress_message())
            #print self.update_progress_message()
        if daemon and self._done_workers != len(self._workers):
            threading.Timer(
                interval=0.3,
                function=self._update_progress,
                kwargs={'daemon': daemon, 'one_time': True}
            ).start()

    def update_progress_message(self):
        self._progress_message = '\n'.join([
            self.update_progress_counts(), '\n',
            self._progress_workers, '\n',
            self.update_progress_time()
        ])
        return self._progress_message

    def update_progress_counts(self):
        self._progress_counts = '/'.join(map(str, [
            len(self._results), self._num_jobs
        ]))
        return self._progress_counts

    def update_progress_workers(self):
        rows = [['Worker', 'Job ID', 'Time'], [''] * 3]
        for w, s in self._ongoing_work.items():
            if s and s['job']:
                rows.append([s['worker'], s['job']['id'], s['time']])
            else:
                rows.append([w, '---', '---'])
        cols = zip(*rows)
        col_widths = [max(len(str(value)) for value in col) for col in cols]
        formatt = '\t\t'.join(['%%%ds' % width for width in col_widths])
        self._progress_workers = '\n'.join([
            formatt % tuple(row) for row in rows
        ])
        return self._progress_workers

    def update_progress_time(self):
        time_tick = time.time()
        tick = datetime.datetime.fromtimestamp(time_tick)
        rd = relativedelta(tick, self.iTime)
        edays = '%d days, ' % (rd.days) if rd.days else ''
        ehours = '%d hours, ' % (rd.hours) if rd.hours else ''
        eminutes = '%d minutes, ' % (rd.minutes) if rd.minutes else ''
        eseconds = '%d seconds' % (rd.seconds)
        len_res = len(self._results)
        if len_res == 0:
            speed = 0.05
        else:
            speed = len_res / (time_tick - self.abs_iTime)
        time_remaining = (self._num_jobs - len(self._results)) / speed
        rt = datetime.datetime.fromtimestamp(time_remaining)
        rdays = '%d days, ' % (rt.day) if rd.days else ''
        rhours = '%d hours, ' % (rt.hour) if rd.hours else ''
        rminutes = '%d minutes, ' % (rt.minute) if rd.minutes else ''
        rseconds = '%d seconds' % (rt.second)
        self._progress_elapsed = ''.join([
            'Elapsed time : ', edays, ehours, eminutes, eseconds,
            '\t\t',
            'Estimated remaining time : ', rdays, rhours, rminutes, rseconds
        ])
        return self._progress_elapsed

    def cleanup(self):
        for worker in self._workers:
            worker.kill()

    def finish(self):
        raise NotImplementedError

    def start(self):
        try:
            for worker in self._workers:
                worker.start()
            self._update_progress(one_time=True, daemon=True)
            while len(self._results) < self._num_jobs:
                if not self._quiet and self._done_workers != len(self._workers):
                    try:
                        state = self._current_queue.get_nowait()
                        if state['time'] is None:
                            self._done_workers += 1
                        self._ongoing_work[state['worker']] = state
                        self.update_progress_workers()
                    except Queue.Empty:
                        pass
                try:
                    self._results.append(self._result_queue.get_nowait())
                except Queue.Empty:
                    time.sleep(0.05)
                    continue
            self.update_progress_workers()
            self._update_progress()
        except KeyboardInterrupt:
            sys.exit(-1)
        except Exception:
            if self._debug:
                traceback.print_exc()
        finally:
            self.finish()
            if not self._debug:
                self.cleanup()
