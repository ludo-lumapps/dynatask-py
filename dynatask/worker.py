from copy import copy
from dataclasses import dataclass
from logging import debug, exception, info, warning
from random import shuffle
from threading import Event, Lock, Thread
from typing import Callable
from zlib import decompress

from valkey import ResponseError, WatchError

from .valkey_stuff import MyValkey
from .shared import (
    GROUP,
    TASK_LOCAL,
    TaskContext,
    WorkConf,
    get_active_job_ids_key,
    get_job_spans_key,
    get_job_stats_key,
    get_stopping_job_ids_key,
    get_stream_key,
    get_stream_watch_key,
    get_valkey,
)


@dataclass
class QueuedTask:
    job_id: int
    task_id: str
    payload: dict[str, bytes]


def xack_entry(
    cli: MyValkey,
    job_type: str,
    local_task_type: str,
    job_id: int,
    task_id: str,
    spans: list[str] | None,
    finished_ok: bool,
):
    # - decreasing all the spans that have this task
    # - xack the task
    pipe = cli.pipeline()
    if spans:
        spans_key = get_job_spans_key(job_type, job_id)
        for span in spans:
            info(f"Decreasing span {span} by 1")
            pipe.pipeline_execute_command("HINCRBY", spans_key, span, "-1")
    done_field = "done_ok" if finished_ok else "done_err"
    stream = get_stream_key(job_type, job_id, local_task_type)
    pipe.pipeline_execute_command("XACK", stream, GROUP, task_id)
    pipe.pipeline_execute_command("XDEL", stream, task_id)
    stats_key = get_job_stats_key(job_type, job_id)
    pipe.pipeline_execute_command("HINCRBY", stats_key, done_field, "1")
    pipe.execute()


def get_task_spans(pl: dict[str, bytes]) -> list[str]:
    if not (lzma_bytes := pl.get("spans")):
        return []
    str_bytes = decompress(lzma_bytes)
    if spans_s := str_bytes.decode():
        return spans_s.split("|")
    else:
        return []


def get_task_data(pl: dict[str, bytes]) -> bytes | None:
    if not (lzma_bytes := pl.get("data")):
        return None
    return decompress(lzma_bytes)


class JobTaskError(Exception):
    def __init__(self, can_retry: bool):
        self.can_retry = can_retry


def process_task(
    task_context: TaskContext,
    params: bytes,
    th: Callable[[bytes], None],
    running_task_ids: dict[int, list[str]],
    running_task_ids_lock: Lock,
):
    TASK_LOCAL.task_context = task_context
    job_type = task_context.job_type
    job_id = task_context.job_id
    task_id = task_context.task_id
    task_type = task_context.task_type
    info(f"Processing job ID {job_id} / task ID {task_id} ")
    with running_task_ids_lock:
        if ids := running_task_ids.get(job_id):
            ids.append(task_id)
        else:
            running_task_ids[job_id] = [task_id]
    xack_ok = None
    try:
        th(params)
        xack_ok = True
    except JobTaskError as err:
        if err.can_retry:
            warning(f"Task {task_id} failed, will retry if max tries not exceeded")
        else:
            warning(f"Task {task_id} failed with no retrying")
            xack_ok = False
    except Exception as err:
        exception(f"Task {task_id} failed with an unhandled error (no retry): {err}")
        xack_ok = False
    if xack_ok is not None:
        xack_entry(
            get_valkey(task_context.valkey_uri),
            job_type,
            task_type,
            job_id,
            task_id,
            task_context.spans,
            xack_ok,
        )
    with running_task_ids_lock:
        running_task_ids[job_id].remove(task_id)


class WorkDispatcher:
    def __init__(
        self,
        conf: WorkConf,
        running_task_ids: dict[int, list[str]],
        running_task_ids_lock: Lock,
        task_handler: Callable[[bytes], None],
    ):
        self.conf = conf
        self.running_task_ids = running_task_ids
        self.running_task_ids_lock = running_task_ids_lock
        self.task_handler = task_handler
        self.stopping_job_ids_key = get_stopping_job_ids_key(conf.job_type)

    def clear_abandoned_tasks_of_stopping_job(
        self, cli: MyValkey, job_id: int, stream: str
    ) -> None:
        # we don't need to handle spans when a job is stopping
        # just xack abandoned tasks
        if not (pels := cli.get_pending_entries(stream, GROUP, "10000")):
            # group gone, job is now done,
            # or nothing to claim
            return
        for pel in pels:
            xack_entry(
                cli,
                self.conf.job_type,
                self.conf.local_task_type,
                job_id,
                pel.id,
                None,
                False,
            )

    def get_next_pel_tasks(
        self,
        cli: MyValkey,
        free_slots: int,
        job_id: int,
        stream: str,
        qts: list[QueuedTask],
    ) -> None:
        # get up to `free_slots` tasks from the PEL
        # without checking for the limit of tasks per job
        # because PEL tasks are already counted in the current nb of tasks
        if not (
            pels := cli.get_pending_entries(stream, GROUP, str(free_slots - len(qts)))
        ):
            # group gone, job is now done,
            # or nothing to claim
            return
        pel_ids = [pel.id for pel in pels]
        # claim them all
        # if race occurs, those that got picked up in the
        #   meantime will not be returned by XCLAIM
        consumer = self.conf.consumer
        if not (entries := cli.claim_entries(stream, GROUP, consumer, pel_ids)):
            return
        max_task_attempts = self.conf.max_task_attempts
        job_type = self.conf.job_type
        task_type = self.conf.local_task_type
        for entry in entries:
            times_deliv = next(e.times_delivered for e in pels if e.id == entry.id)
            if times_deliv >= max_task_attempts:
                warning(f"Task {entry.id} tried too many times, deleting it")
                try:
                    spans = get_task_spans(entry.data)
                except Exception as err:
                    warning(
                        f"Error '{err}' parsing spans of {entry.id}, "
                        f"will assume no spans to XACK this errored task"
                    )
                    spans = None
                xack_entry(cli, job_type, task_type, job_id, entry.id, spans, False)
                continue
            qts.append(QueuedTask(job_id, entry.id, entry.data))
            info(f"Entry {entry.id} of stream {stream} was claimed by {consumer}")
            if len(qts) >= free_slots:
                break

    def get_pending_count(self, cli: MyValkey, stream: str) -> int:
        # [
        #   3,
        #   b'1756797922859-0',
        #   b'1756798216490-0',
        #   [
        #       [b'consumer-1', b'2'],
        #       [b'consumer-2', b'1']
        #   ]
        # ]
        #       or
        # [0, None, None, None]
        resp = cli.cmd("XPENDING", stream, GROUP)
        return resp[0]

    def get_next_new_tasks(
        self,
        cli: MyValkey,
        free_slots: int,
        job_id: int,
        stream: str,
        qts: list[QueuedTask],
    ) -> None:
        watch_key = get_stream_watch_key(stream)
        pipe = cli.pipeline()
        pipe.execute_command("WATCH", watch_key)
        pending_count = self.get_pending_count(cli, stream)
        if pending_count >= self.conf.max_tasks_per_job:
            return
        left_for_stream = self.conf.max_tasks_per_job - pending_count
        max_to_get = min(left_for_stream, free_slots - len(qts))
        pipe.multi()
        pipe.pipeline_execute_command("INCR", watch_key)
        pipe.pipeline_execute_command(
            "XREADGROUP",
            "GROUP",
            GROUP,
            self.conf.consumer,
            "COUNT",
            str(max_to_get),
            "STREAMS",
            stream,
            ">",
        )
        try:
            resp = pipe.execute()
        except WatchError:
            # watched value changed
            return  # could retry ?
        except ResponseError as err:
            if "NOGROUP" in str(err):
                return
            raise
        """ resp:
            [
                2,
                [
                    [
                        b'stream-1',
                        [
                            (b'1756798216723-0', {b'key1', b'val1', b'key2', b'val2'}),
                            (b'1756798216878-0', {b'key1', b'val1', b'key2', b'val2'}),
                            (b'1756815635488-0', {b'foo', b'bar'})
                        ]
                    ]
                ]
            ]
                    or
            [0, []]
                    or
            [0, [b'stream-1', []]]
        """
        try:
            entries = resp[1][0][1]
        except IndexError:
            return
        read_count = len(entries)
        for entry in entries:
            qts.append(
                QueuedTask(
                    job_id,
                    entry[0].decode(),
                    {k.decode(): v for k, v in entry[1].items()},
                )
            )
        cli.cmd(
            "HINCRBY",
            get_job_stats_key(self.conf.job_type, job_id),
            "read",
            str(read_count),
        )

    def is_stopping(self, cli: MyValkey, job_id: int) -> bool:
        return bool(cli.cmd("SISMEMBER", self.stopping_job_ids_key, job_id))

    def get_next_tasks(self, free_slots: int) -> list[QueuedTask] | None:
        job_type = self.conf.job_type
        task_type = self.conf.local_task_type
        cli = get_valkey(self.conf.valkey_uri)
        if not (
            job_ids := cli.cmd("SRANDMEMBER", get_active_job_ids_key(job_type), "10")
        ):
            return None
        qts: list[QueuedTask] = []
        shuffle(job_ids)
        for job_id in job_ids:
            stream = get_stream_key(job_type, job_id, task_type)
            if self.is_stopping(cli, job_id):
                self.clear_abandoned_tasks_of_stopping_job(cli, job_id, stream)
                continue
            self.get_next_pel_tasks(cli, free_slots, job_id, stream, qts)
            if len(qts) >= free_slots:
                break
            self.get_next_new_tasks(cli, free_slots, job_id, stream, qts)
            if len(qts) >= free_slots:
                break
        return qts

    def process_tasks(self, qts: list[QueuedTask], tasks: list[Thread]) -> None:
        consumer = self.conf.consumer
        job_type = self.conf.job_type
        task_type = self.conf.local_task_type
        th = self.task_handler
        valkey_uri = self.conf.valkey_uri
        cli = get_valkey(valkey_uri)
        for qt in qts:
            job_id = qt.job_id
            task_id = qt.task_id
            try:
                spans = get_task_spans(qt.payload)
            except Exception as err:
                warning(f"Error '{err}' parsing spans of {task_id}")
                try:
                    xack_entry(cli, job_type, task_type, job_id, task_id, None, False)
                except Exception as err:
                    warning(f"Error XACK'ing task {task_id} of job {job_id}: {err}")
                continue
            if spans:
                debug(f"Task {task_id} has spans {spans}")
            else:
                debug(f"Task {task_id} has no spans")
            err_msg = None
            try:
                if not (params := get_task_data(qt.payload)):
                    err_msg = "payload missing from entry"
            except Exception as err:
                err_msg = f"Error '{err}' parsing payload of {task_id}"
            if err_msg:
                warning(err_msg)
                try:
                    xack_entry(cli, job_type, task_type, job_id, task_id, None, False)
                except Exception as err:
                    warning(f"Error XACK'ing task {task_id} of job {job_id}: {err}")
                continue
            trace_id = f"trace-worker-{consumer}-job-{job_id}-task-{task_id}"
            task_context = TaskContext(
                valkey_uri, job_type, job_id, task_id, task_type, spans
            )
            thread = Thread(
                target=process_task,
                name=trace_id,
                args=[
                    task_context,
                    params,
                    th,
                    self.running_task_ids,
                    self.running_task_ids_lock,
                ],
            )
            thread.start()
            tasks.append(thread)

    def dispatch_work(self, exit_flag: Event):
        activity_sleep_s = 0.100
        inactivity_sleep_s = 2.0
        info("Starting work dispatch")
        max_slots = self.conf.thread_count
        tasks: list[Thread] = []
        next_sleep_s = 0.0
        while not exit_flag.wait(timeout=next_sleep_s):
            for t in copy(tasks):
                if not t.is_alive():
                    tasks.remove(t)
            free_slots = max_slots - len(tasks)
            if free_slots <= 0:
                next_sleep_s = activity_sleep_s
                continue
            try:
                qts = self.get_next_tasks(free_slots)
                if qts:
                    self.process_tasks(qts, tasks)
                    # stuff is happening, don't sleep too long...
                    next_sleep_s = activity_sleep_s
                elif qts is not None:
                    # got no tasks, but stuff may be happening,
                    next_sleep_s = activity_sleep_s
                else:
                    # nothing to do, sleep a little long...
                    next_sleep_s = inactivity_sleep_s
            except Exception as err:
                exception(f"Error getting up to {free_slots} next tasks: {err}")
                next_sleep_s = 5.0
        for t in tasks:
            t.join()
        info("Exiting work dispatch")
