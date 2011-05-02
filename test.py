#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from multiworker import Controller, Worker


class CustomWorker(Worker):
    def __init__(self, work_queue, result_queue, verbose, global_params):
        super(Worker, self).__init__(work_queue, result_queue, verbose, global_params)
        print 'custom worker created'

if __name__ == '__main__':
    global_params = {'fdsgdf':3, 'kgflgf':5}
    jobs = [{'name':'lskdfjs'}, {'name':'ksljdfs'}, {'name':'ksjdnfs'}, {'name':'ggtuna'}]
    c = Controller(jobs, global_params, 1, True, CustomWorker)
    c.start()
