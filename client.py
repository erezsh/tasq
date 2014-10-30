from django.utils import timezone

from utils import pack_args

from .models import Task


def add_task(func, args=(), kwargs=None, subtasks=(),
             name=None, start_at=None, time_limit=None, priority=None):


    task = Task()
    task.func_module = func.__module__
    task.func_name = func.__name__
    task.args = pack_args(args, kwargs or {})
    task.name = name
    task.start_at = start_at or timezone.now()
    task.time_limit = time_limit
    task.priority = priority or 0
    task.save()

    for subtask in subtasks:
        task.subtasks.append(subtask)

    task.status = 'P'
    task.save()
    return task

