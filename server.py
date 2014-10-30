import time
import json
from django.utils import timezone
from datetime import timedelta
from threading import Thread
import logging

from django.db import transaction

from .utils import unpack_args, find_function
from .models import Task, Worker

HOST = 'localhost'
WORKERS = 2
LAG = 1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('tasq')
logger.setLevel(logging.DEBUG)
log = logger.info

class NoTasksPending(Exception):
    pass

class WorkerThread(Thread):
    def __init__(self):
        self.worker = Worker(host=HOST)
        self.worker.save()

        Thread.__init__(self, name='Worker-%d' % self.worker.id)

    def _next_task(self):
        with transaction.atomic():
            pending_tasks = Task.objects.filter(status='P', start_at__lte=timezone.now()).order_by('-priority')
            next_task_q = pending_tasks.select_for_update()[:1]
            next_task = next_task_q.first()
            if next_task is None:
                raise NoTasksPending()

            assert next_task.status == 'P'
            next_task.status = 'R'
            next_task.worker = self.worker
            next_task.save()

            return next_task

    def _run_task(self, task):
        func = find_function(task.func_module, task.func_name)
        args, kwargs = unpack_args(task.args)
        return func(task, *args, **kwargs)

    def run(self):
        log('Worker %s starting', self.worker)

        self.worker.started = timezone.now()
        self.worker.status = 'i'
        self.worker.save()

        while True:
            logger.debug('Waiting for next task')
            try:
                task = self._next_task()
            except NoTasksPending:
                time.sleep(LAG)
                continue

            log('Accepted task: %s', task)
            self.worker.current_task = task
            self.worker.status = 'R'
            self.worker.save()

            try:
                task.result_str = json.dumps(self._run_task(task))
                task.status = 'S'
                log('Finished task successfully: %s', task)
            except Exception, e:
                task.error_str = unicode(e)
                task.status = 'E'
                log('Error in task: %s', task)

            task.ended_at = timezone.now()
            task.save()

            self.worker.status = 'i'
            self.worker.save()


def main():
    log('Starting server')

    # start worker threads
    workers = [WorkerThread() for x in range(WORKERS)]

    for worker in workers:
        worker.setDaemon(True)
        worker.start()

    for worker in workers:
        worker.join()

