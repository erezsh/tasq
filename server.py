import time
import json
from django.utils import timezone
from threading import Thread
from multiprocessing import Process, Value
import logging

from django.db import transaction, OperationalError, InterfaceError
from django.db import connection

from .utils import unpack_args, find_function
from .models import Task, Worker

HOST = 'localhost'
WORKERS = 4
LAG = 1
TIMEOUT = 20 * 60
TIMEOUT_LAG = LAG * 10

g_ctrl_c = Value('i', False)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('tasq')
logger.setLevel(logging.INFO)
log = logger.info

class NoTasksPending(Exception):
    pass

class WorkerProcess(Process):
    def __init__(self):
        self.worker = Worker(host=HOST)
        self.worker.save()

        Process.__init__(self,
                target = self.main,
                args = (g_ctrl_c, ),
                name='Worker-%d' % self.worker.id
            )

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

    def _mainloop(self):
        while True:
            logger.debug('Waiting for next task')
            try:
                task = self._next_task()
            except NoTasksPending:
                time.sleep(LAG)
                continue
            except OperationalError as e:
                logger.exception(e)
                time.sleep(LAG)
                continue

            log('Accepted task: %s', task)
            self.worker.current_task = task
            self.worker.status = 'R'
            self.worker.save()

            task.started_at = timezone.now()
            task.save()

            connection.close()
            try:
                json_result = json.dumps(self._run_task(task))
                connection.close()
                task = task.reload_as_new()
                task.result_str = json_result
                task.status = 'S'
                log('Finished task successfully: %s', task)
            except Exception, e:
                connection.close()
                task = task.reload_as_new()
                task.error_str = unicode(e)
                task.status = 'E'
                logger.exception(e)
                log('Error in task: %s', task)


            task.ended_at = timezone.now()
            task.save()

            self.worker.status = 'i'
            self.worker.save()

    def main(self, ctrl_c):
        connection.close()
        try:
            log('Worker process %s starting', self.worker)

            self.worker.started = timezone.now()
            self.worker.status = 'i'
            self.worker.save()

            self._mainloop()

        except KeyboardInterrupt:
            ctrl_c.value = True
            raise

class WorkerThread(Thread):
    @staticmethod
    def _new_process():
        # connection.close()
        process = WorkerProcess()
        process.start()
        connection.close()
        return process

    def run(self):
        process = self._new_process()

        try:
            while True:
                process.join(TIMEOUT_LAG)
                if g_ctrl_c.value:
                    return

                if process.is_alive():
                    worker = process.worker.reload_as_new()
                    if worker.current_task:
                        task_duration = (timezone.now() - worker.current_task.started_at).total_seconds()
                        if task_duration > TIMEOUT:
                            process.terminate()
                            process.join()

                            worker.current_task.report_error('Timeout %s' % TIMEOUT)
                            worker.current_task.save()

                if not process.is_alive():
                    process.worker.status = 'O'
                    process.worker.save()

                    process = self._new_process()
        except Exception, e:
            logger.exception(e)
            if process.worker.current_task:
                process.worker.current_task.report_error('Worker crashed: %s' % e)
                process.worker.current_task.save()

        finally:
            process.join()
            process.worker.status = 'O'
            try:
                process.worker.save()
            except InterfaceError, e:
                # Database is already closed
                # TODO: wait and retry?
                logger.exception(e)


def main():
    log('Starting server')

    # start worker threads
    worker_threads = [WorkerThread() for _ in range(WORKERS)]

    for worker in worker_threads:
        # worker.setDaemon(True)
        worker.start()

    for worker in worker_threads:
        worker.join()

    log('Shutting down')

