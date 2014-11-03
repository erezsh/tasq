import time

from .client import add_task

def add(task, a, b):
    return a + b

def error(task, x):
    raise ValueError(x)

def sleeper(task, x):
    time.sleep(x)

def run_basic():
    start = time.time()

    TASKS = 100
    tasks = [add_task(add, (2, i)) for i in range(TASKS)]

    while any(task.status == 'P' for task in tasks):
        tasks = [task.reload_as_new() for task in tasks]

    for i, task in enumerate(tasks):
        assert task.status == 'S', task.status
        assert task.result_str == unicode(2 + i)

    end = time.time()

    print "Took %.2f seconds" % (end - start)

# def test_timeout():
#     add_task(sleeper, (20),)

