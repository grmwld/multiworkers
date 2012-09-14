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
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.current_queue = current_queue
        self.global_params = global_params
        self.kill_received = False

    def kill(self):
        self.kill_received = True
        self.terminate()

    def update_progress(self, interval=1):
        tick = datetime.datetime.fromtimestamp(time.time())
        rd = relativedelta(tick, self.iTime)
        days = '%dd ' % (rd.days) if rd.days else ''
        hours = '%dh ' % (rd.hours) if rd.hours else ''
        minutes = '%dm ' % (rd.minutes) if rd.minutes else ''
        seconds = '%ds' % (rd.seconds)
        elapsed = ''.join([days, hours, minutes, seconds])
        self.current_queue.put({
            'job': self.job,
            'worker': self.name,
            'time': elapsed
        })
        self.looper = threading.Timer(
            interval=interval,
            function=self.update_progress,
            kwargs={'interval': interval}
        ).start()

    def run(self):
        self.iTime = datetime.datetime.fromtimestamp(time.time())
        self.job = None
        self.update_progress()
        while not self.kill_received:
            try:
                self.job = self.work_queue.get(True)
                self.iTime = datetime.datetime.fromtimestamp(time.time())
                if self.job is None:
                    raise Queue.Empty
                result = self.do(self.job)
                self.result_queue.put(result)
            except (Queue.Empty, KeyboardInterrupt):
                break
        self.current_queue.put({
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
        self.jobs = jobs
        self.global_params = global_params
        self.num_cpu = num_cpu
        self.quiet = quiet
        self.worker_class = worker_class
        self.work_queue = multiprocessing.Queue()
        self.debug = debug
        self.result_queue = multiprocessing.Queue()
        self.current_queue = multiprocessing.Queue()
        self.num_jobs = 0
        for job in self.jobs:
            job['id'] = self.num_jobs
            self.work_queue.put(job)
            self.num_jobs += 1
        self.results = []
        self.workers = []
        self.ongoing_work = {}
        self.init_workers()
        self.done_workers = 0
        self.progress = ProgressBar('green', width=80, block='█', empty='█')
        self.progress_time = ''
        self.progress_counts = ''
        self.progress_workers = ''
        self.progress_message = ''
        self.abs_iTime = time.time()
        self.iTime = datetime.datetime.fromtimestamp(self.abs_iTime)

    def init_workers(self):
        for i in range(self.num_cpu):
            worker = self.worker_class(
                self.work_queue,
                self.result_queue,
                self.current_queue,
                self.global_params,
            )
            self.workers.append(worker)
            self.work_queue.put(None)
            self.ongoing_work[worker.name] = {}

    def update_progress(self, one_time=True, daemon=False):
        if one_time:
            percent = int((len(self.results) / float(self.num_jobs)) * 100)
            self.progress.render(percent, self.update_progress_message())
        if daemon and self.done_workers < len(self.workers):
            threading.Timer(
                interval=.2,
                function=self.update_progress,
                kwargs={'daemon': daemon, 'one_time': True}
            ).start()

    def update_progress_message(self):
        self.progress_message = '\n'.join([
            self.update_progress_counts(), '\n',
            self.progress_workers, '\n',
            self.update_progress_time(), '\n'
        ])
        return self.progress_message

    def update_progress_counts(self):
        self.progress_counts = '/'.join(map(str, [
            len(self.results), self.num_jobs
        ]))
        return self.progress_counts

    def update_progress_workers(self):
        rows = [[' ' * 20 + 'Worker', 'Job ID', 'Time'], [''] * 3]
        for w, s in self.ongoing_work.items():
            if s and s['job']:
                rows.append([s['worker'], s['job']['id'], s['time']])
            else:
                rows.append([w, '---', '---'])
        cols = zip(*rows)
        col_widths = [max(len(str(value)) for value in col) for col in cols]
        formatt = '\t\t'.join(['%%%ds' % width for width in col_widths])
        self.progress_workers = '\n'.join([
            formatt % tuple(row) for row in rows
        ])
        return self.progress_workers

    def update_progress_time(self):
        time_tick = time.time()
        tick = datetime.datetime.fromtimestamp(time_tick)
        rd = relativedelta(tick, self.iTime)
        edays = '%d days, ' % (rd.days) if rd.days else ''
        ehours = '%d hours, ' % (rd.hours) if rd.hours else ''
        eminutes = '%d minutes, ' % (rd.minutes) if rd.minutes else ''
        eseconds = '%d seconds' % (rd.seconds)
        len_res = len(self.results)
        if len_res == 0:
            speed = 0.05
        else:
            speed = len_res / (time_tick - self.abs_iTime)
        time_remaining = (self.num_jobs - len(self.results)) / speed
        rt = datetime.datetime.fromtimestamp(time_remaining)
        rdays = '%d days, ' % (rt.day) if rd.days else ''
        rhours = '%d hours, ' % (rt.hour) if rd.hours else ''
        rminutes = '%d minutes, ' % (rt.minute) if rd.minutes else ''
        rseconds = '%d seconds' % (rt.second)
        self.progress_elapsed = ''.join([
            'Elapsed time : ', edays, ehours, eminutes, eseconds,
            '\t\t',
            'Estimated remaining time : ', rdays, rhours, rminutes, rseconds
        ])
        return self.progress_elapsed

    def cleanup(self):
        for worker in self.workers:
            worker.kill()

    def finish(self):
        raise NotImplementedError

    def start(self):
        try:
            for worker in self.workers:
                worker.start()
            self.update_progress(one_time=True, daemon=True)
            while self.done_workers < len(self.workers):
                if not self.quiet:
                    try:
                        state = self.current_queue.get_nowait()
                        if state['time'] is None:
                            self.done_workers += 1
                        self.ongoing_work[state['worker']] = state
                        self.update_progress_workers()
                    except Queue.Empty:
                        time.sleep(0.05)
                try:
                    self.results.append(self.result_queue.get_nowait())
                except Queue.Empty:
                    time.sleep(0.05)
            #self.update_progress_workers()
            #self.update_progress()
        except KeyboardInterrupt:
            sys.exit(-1)
        except Exception:
            if self.debug:
                traceback.print_exc()
        finally:
            time.sleep(1)
            self.finish()
            if not self.debug:
                self.cleanup()


def main():

    c = CustomController(
        jobs=[{'name': str(hex(i)), 'value': i} for i in range(100)],
        global_params={},
        num_cpu=multiprocessing.cpu_count(),
        quiet=False,
        worker_class=CustomWorker,
        debug=True
    )
    c.start()


if __name__ == '__main__':

    import random

    class CustomWorker(Worker):
        def __init__(self, work_queue, result_queue, current_queue, global_params):
            Worker.__init__(self, work_queue, result_queue, current_queue, global_params)

        def do(self, job):
            result = {'transformed_value': job['value'] ** 2.3 + 3.4}
            time.sleep(0.5 + random.random())
            return result

    class CustomController(Controller):
        def __init__(self, jobs, global_params, num_cpu, quiet, worker_class, debug):
            Controller.__init__(self, jobs, global_params, num_cpu, quiet, worker_class, debug)

        def finish(self):
            output = sum(result['transformed_value'] for result in self.results)
            print output

    main()
