#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import time
import random
from multiworker import Controller, Worker


class CustomWorker(Worker):
    def __init__(self, work_queue, result_queue, verbose, global_params):
        Worker.__init__(self, work_queue, result_queue, verbose, global_params)
        print 'custom worker created'

    def do(self, job):
        result = job
        result.update(self._global_params)
        time.sleep(1 + random.random())
        print 'done', job
        return result

if __name__ == '__main__':
    global_params = {'fdsgdf':3, 'kgflgf':5}
    jobs = [{'name':'lskdfjs'}, {'name':'ksljdfs'}, {'name':'ksjdnfs'}, {'name':'ggtuna'}]
    c = Controller(jobs, global_params, 2, True, CustomWorker)
    c.start()
