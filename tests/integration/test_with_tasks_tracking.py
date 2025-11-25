import re
from logging import debug
from random import randint
from threading import Event, Thread
from time import sleep

import msgspec
from valkey import Valkey

from dynatask import WorkConf, start_work, FinishedJobInfo, InTaskContext, Client

VALKEY_URI = "redis://127.0.0.1:6379/3"
JOB_TYPE = "test_job_trackers"
LOCAL_TASK_TYPE = "default"


class ValkeyCounter:
    def __init__(self, key: str):
        self.valkey_pool = Valkey.from_url(VALKEY_URI)
        self.key = key

    def reset(self):
        self.valkey_pool.set(self.key, 0)

    def incr(self, by: int):
        self.valkey_pool.incrby(self.key, by)

    def get(self) -> int:
        return int(self.valkey_pool.get(self.key) or 0)  # type: ignore


def job_is_done(job_info: FinishedJobInfo):
    pass


def snake_case(s: str):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


class FooAll(msgspec.Struct, tag=True, tag_field="name"):
    item_count: int


class FooItem(msgspec.Struct, tag=True, tag_field="name"):
    item_id: str


class TaskContext(msgspec.Struct):
    project_id: int
    counter_key: str


class TaskMessage(msgspec.Struct):
    context: TaskContext
    method: FooAll | FooItem


class Task:
    def __init__(self, ctxt: TaskContext):
        self.ctxt = ctxt

    def foo_all(self, item_count: int):
        debug(f"FooAll method invoked, project_id is: {self.ctxt.project_id}")
        for i in range(item_count):
            new_msg = TaskMessage(self.ctxt, FooItem(f"ITEM_{i}"))
            InTaskContext.add_task(LOCAL_TASK_TYPE, msgspec.json.encode(new_msg), True)
        InTaskContext.wait_until_tracked_tasks_done()

    def foo_item(self, item_id: str):
        debug(
            f"FooItem item_id={item_id} invoked, project_id is: {self.ctxt.project_id}"
        )
        nb_secs = randint(1, 10)
        debug(f"Sleeping {nb_secs} seconds")
        sleep(nb_secs)
        debug("Task is done sleeping")
        ValkeyCounter(self.ctxt.counter_key).incr(1)
        debug("Task is done")


def test_1():
    data = b'{"context": {"project_id": -1, "counter_key": "FOO"}, "method": {"name": "FooAll", "item_count": 4}}'
    t = msgspec.json.decode(data, type=TaskMessage)
    assert t.context.project_id == -1
    ok = False
    match t.method:
        case FooAll(_foo):
            ok = True
    assert ok


def test_with_tasks_tracking():
    def process_task(data: bytes):
        task_msg = msgspec.json.decode(data, type=TaskMessage)
        task = Task(task_msg.context)
        debug(f"Handling task {task}")
        match task_msg.method:
            case FooAll(item_count=item_count):
                task.foo_all(item_count)
            case FooItem(item_id=item_id):
                task.foo_item(item_id)

    conf = WorkConf(VALKEY_URI, JOB_TYPE, LOCAL_TASK_TYPE)
    conf.max_task_attempts = 1
    conf.max_tasks_per_job = 5
    conf.thread_count = 5
    counter_key = "TEST_WITH_TRACKING_COUNTER"
    valkey_context = ValkeyCounter(counter_key)
    valkey_context.reset()
    exit_flag = Event()
    workers_handle = Thread(
        target=start_work, args=(conf, job_is_done, process_task, exit_flag)
    )
    workers_handle.start()
    debug("started workers")
    job_id = 1234
    item_count_orig = 10
    new_msg = TaskMessage(TaskContext(job_id, counter_key), FooAll(item_count_orig))
    jobs_client = Client(VALKEY_URI, JOB_TYPE, [LOCAL_TASK_TYPE])
    jobs_client.start_job(job_id, LOCAL_TASK_TYPE, msgspec.json.encode(new_msg))
    while jobs_client.job_is_running(job_id):
        sleep(1)
    exit_flag.set()
    debug("Waiting for worker to exit")
    workers_handle.join()
    debug("Worker has exited")
    item_count_final = valkey_context.get()
    assert item_count_orig == item_count_final
